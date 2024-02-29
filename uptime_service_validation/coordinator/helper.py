"""This module contains various helper functions and classes for the
coordinator."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import psycopg2
from psycopg2 import extras
import requests


ERROR = "Error: {0}"


@dataclass
class Batch:
    """Represents the timeframe of the current batch and the database
    identifier of the previous batch for reference."""
    start_time: datetime
    bot_log_id: int
    interval: timedelta

    @property
    def end_time(self):
        "Return the end time of the batch."
        return self.start_time + self.interval

    def next(self, bot_log_id):
        "Return an object representing the next batch."
        return self.__class__(
            start_time=self.end_time,
            interval=self.interval,
            bot_log_id=- bot_log_id
        )

    def split(self, parts_number):
        "Splits the batch time window into equal parts for parallel proecessing."
        diff = (self.end_time - self.start_time) / parts_number
        return ((self.start_time + diff * i, self.start_time + diff * (i + 1))
                for i in range(parts_number))


class DB:
    """A wrapper around the database connection, providing high-level methods
    for querying and updating the database."""

    def __init__(self, connection, logger):
        self.connection = connection
        self.logger = logger

    def get_batch_timings(self, interval):
        "Get the time-frame for the next batch and previous batch's id."
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT id, batch_end_epoch FROM bot_logs ORDER BY batch_end_epoch DESC limit 1 "
            )
            result = cursor.fetchone()
            bot_log_id = result[0]
            prev_epoch = result[1]

            prev_batch_end = datetime.fromtimestamp(prev_epoch, timezone.utc)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            raise RuntimeError("Could not load the latest batch.") from error
        finally:
            cursor.close()
        return Batch(
            start_time=prev_batch_end,
            bot_log_id=bot_log_id,
            interval=interval
        )

    def get_previous_statehash(self, bot_log_id):
        "Get the statehash of the latest batch."
        cursor = self.connection.cursor()
        try:
            sql_query = """select  ps.value parent_statehash, s1.value statehash, b.weight
                        from bot_logs_statehash b join statehash s1 ON s1.id = b.statehash_id
                        join statehash ps on b.parent_statehash_id = ps.id where bot_log_id =%s"""
            cursor.execute(sql_query, (bot_log_id,))
            result = cursor.fetchall()

            df = pd.DataFrame(
                result, columns=["parent_state_hash", "state_hash", "weight"])
            previous_result_df = df[["parent_state_hash", "state_hash"]]
            p_selected_node_df = df[["state_hash", "weight"]]
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return -1
        finally:
            cursor.close()
        return previous_result_df, p_selected_node_df

    def get_statehash_df(self):
        "Get the list of all known statehashes as a data frame."
        cursor = self.connection.cursor()
        try:
            cursor.execute("select value from statehash")
            result = cursor.fetchall()
            state_hash = pd.DataFrame(result, columns=["statehash"])
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            return -1
        finally:
            cursor.close()
        return state_hash

    def create_statehash(self, statehash_df, page_size=100):
        "Add a new statehashto the database."
        tuples = [tuple(x) for x in statehash_df.to_numpy()]
        self.logger.info("create_statehash: %s", tuples)
        query = """INSERT INTO statehash ( value)
                VALUES ( %s)  """
        cursor = self.connection.cursor()
        try:
            extras.execute_batch(cursor, query, tuples, page_size)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return -1
        finally:
            cursor.close()
        self.logger.info("create_statehash  end ")
        return 0

    def create_node_record(self, df, page_size=100):
        "Add new block producers to the database."
        self.logger.info("create_point_record  start ")
        tuples = [tuple(x) for x in df.to_numpy()]
        query = """INSERT INTO nodes ( block_producer_key, updated_at)
                VALUES ( %s,  %s )  """
        cursor = self.connection.cursor()
        try:
            extras.execute_batch(cursor, query, tuples, page_size)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return 1
        finally:
            cursor.close()
        self.logger.info("create_point_record  end ")
        return 0

    def create_bot_log(self, values):
        "Add a new batch to the database."
        self.logger.info("create_bot_log  start ")
        query = """INSERT INTO bot_logs(files_processed, file_timestamps, batch_start_epoch, batch_end_epoch,
                processing_time)  values ( %s, %s, %s, %s, %s) RETURNING id """
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, values)
            result = cursor.fetchone()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return -1
        finally:
            cursor.close()
        self.logger.info("create_bot_log  end ")
        return result[0]

    def insert_statehash_results(self, df, page_size=100):
        "Relate statehashes to the batches they were observed in."
        self.logger.info("create_botlogs_statehash  start ")
        temp_df = df[["parent_state_hash",
                      "state_hash", "weight", "bot_log_id"]]
        tuples = [tuple(x) for x in temp_df.to_numpy()]
        query = """INSERT INTO bot_logs_statehash(parent_statehash_id, statehash_id, weight, bot_log_id )
                VALUES (
                  (SELECT id FROM statehash WHERE value= %s),
                  (SELECT id FROM statehash WHERE value= %s),
                  %s,
                  %s ) """
        cursor = self.connection.cursor()

        try:
            extras.execute_batch(cursor, query, tuples, page_size)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return 1
        finally:
            cursor.close()
        self.logger.info("create_botlogs_statehash  end ")
        return 0

    def create_point_record(self, df, page_size=100):
        "Add a new scoring submission to the database."
        self.logger.info("create_point_record  start ")
        tuples = [tuple(x) for x in df.to_numpy()]
        query = """INSERT INTO points
                   (file_name, file_timestamps, blockchain_epoch, node_id,
                   blockchain_height, amount, created_at, bot_log_id, statehash_id)
                VALUES ( %s, %s,  %s, (SELECT id FROM nodes WHERE block_producer_key= %s),
                         %s, %s, %s, %s, (SELECT id FROM statehash WHERE value= %s) )"""
        try:
            cursor = self.connection.cursor()
            extras.execute_batch(cursor, query, tuples, page_size)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return 1
        finally:
            cursor.close()
        self.logger.info("create_point_record  end ")
        return 0

    def update_scoreboard(self, score_till_time, uptime_days=30):
        "Update the block producer scores."
        self.logger.info("updateScoreboard  start ")
        sql = """with vars  (snapshot_date, start_date) as( values (%s AT TIME ZONE 'UTC',
                        (%s - interval '%s' day) AT TIME ZONE 'UTC')
              )
              , epochs as(
                select extract('epoch' from snapshot_date) as end_epoch,
                extract('epoch' from start_date) as start_epoch from vars
              )
              , b_logs as(
                select (count(1) ) as surveys
                from bot_logs b , epochs e
                where b.batch_start_epoch >= start_epoch and  b.batch_end_epoch <= end_epoch
              )
              , scores as (
                select p.node_id, count(p.bot_log_id) bp_points
                from points p join bot_logs b on p.bot_log_id =b.id, epochs
                where b.batch_start_epoch >= start_epoch and  b.batch_end_epoch <= end_epoch
                group by 1
              )
              , final_scores as (
                select node_id, bp_points,
                surveys, trunc( ((bp_points::decimal*100) / surveys),2) as score_perc
                from scores l join nodes n on l.node_id=n.id, b_logs t
              )
              update nodes nrt set score = s.bp_points, score_percent=s.score_perc
              from final_scores s where nrt.id=s.node_id """

        history_sql = """insert into score_history (node_id, score_at, score, score_percent)
                      SELECT id as node_id, %s, score, score_percent from nodes where score is not null """
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                sql,
                (
                    score_till_time,
                    score_till_time,
                    uptime_days,
                ),
            )
            cursor.execute(history_sql, (score_till_time,))
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return -1
        finally:
            cursor.close()
        self.logger.info("updateScoreboard  end ")
        return 0

    def get_existing_nodes(self):
        "Get the list of all known block producers."
        cursor = self.connection.cursor()
        try:
            cursor.execute("select block_producer_key from nodes")
            result = cursor.fetchall()
            nodes = pd.DataFrame(result, columns=["block_producer_key"])
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            return -1
        finally:
            cursor.close()
        return nodes


def get_relations(df):
    "Extract parent-child relations between statehashes oin a dataframe."
    return ((parent, child) for child, parent
            in df[["state_hash", "parent_state_hash"]].values
            if parent in df["state_hash"].values)


def find_new_values_to_insert(existing_values, new_values):
    "Find the new values to insert into the database."
    return (
        existing_values.merge(new_values, how="outer", indicator=True)
        .loc[lambda x: x["_merge"] == "right_only"]
        .drop("_merge", axis=1)
        .drop_duplicates()
    )


def filter_state_hash_percentage(df, p=0.34):
    "Filter statehashes by percentage of block producers who submitted them."
    state_hash_list = (
        df["state_hash"].value_counts().sort_values(
            ascending=False).index.to_list()
    )
    # get 34% number of blk in given batch
    total_unique_blk = df["block_producer_key"].nunique()
    percentage_result = round(total_unique_blk * p, 2)
    good_state_hash_list = list()
    for s in state_hash_list:
        blk_count = df[df["state_hash"] == s]["block_producer_key"].nunique()
        # check blk_count for state_hash submitted by blk least 34%
        if blk_count >= percentage_result:
            good_state_hash_list.append(s)
    return good_state_hash_list


def create_graph(batch_df, p_selected_node_df, c_selected_node, p_map):
    """Create a directed graph of parent-child relations between blocks
    in the batch dataframe."""
    batch_graph = nx.DiGraph()
    parent_hash_list = batch_df["parent_state_hash"].unique()
    state_hash_list = set(
        list(batch_df["state_hash"].unique())
        + list(p_selected_node_df["state_hash"].values)
    )
    selected_parent = [
        parent for parent in parent_hash_list if parent in state_hash_list
    ]

    batch_graph.add_nodes_from(list(p_selected_node_df["state_hash"].values))
    batch_graph.add_nodes_from(c_selected_node)
    batch_graph.add_nodes_from(state_hash_list)

    for row in batch_df.itertuples():
        state_hash = getattr(row, "state_hash")
        parent_hash = getattr(row, "parent_state_hash")

        if parent_hash in selected_parent:
            batch_graph.add_edge(parent_hash, state_hash)

    #  add edges from previous batch nodes
    batch_graph.add_edges_from(p_map)

    return batch_graph


def apply_weights(batch_graph, c_selected_node, p_selected_node):
    "Apply weights to to statehashes,"
    for node in list(batch_graph.nodes()):
        if node in c_selected_node:
            batch_graph.nodes[node]["weight"] = 0
        elif node in p_selected_node["state_hash"].values:
            batch_graph.nodes[node]["weight"] = p_selected_node[
                p_selected_node["state_hash"] == node
            ]["weight"].values[0]
        else:
            batch_graph.nodes[node]["weight"] = 9999

    return batch_graph


def plot_graph(batch_graph, g_pos, title):
    "Plot the graph of parent-child relations between state hashes."
    # plot the graph
    plt.figure(figsize=(8, 8))
    plt.title(title)
    if not g_pos:
        g_pos = nx.spring_layout(batch_graph, k=0.3, iterations=1)
    nx.draw(
        batch_graph,
        pos=g_pos,
        connectionstyle="arc3, rad = 0.1",
        with_labels=False,
        node_size=500,
    )
    t_dict = {}
    for n in batch_graph.nodes:
        if batch_graph.nodes[n]["weight"] == 0:
            t_dict[n] = (n[-4:], 0)
        else:
            t_dict[n] = (n[-4:], batch_graph.nodes[n]["weight"])
    nx.draw_networkx_labels(batch_graph, pos=g_pos, labels=t_dict, font_size=8)
    plt.show()  # pause before exiting
    return g_pos


def get_minimum_weight(graph, child_node):
    "Find the statehash with the minimum weight."
    child_node_weight = graph.nodes[child_node]["weight"]
    for parent in list(graph.predecessors(child_node)):
        lower = min(graph.nodes[parent]["weight"] + 1, child_node_weight)
        child_node_weight = lower
    return child_node_weight


def bfs(graph, queue_list, node, max_depth=2):
    "Breadth-first search through the graph."
    visited = list()
    visited.append(node)
    cnt = 2
    while queue_list:
        m = queue_list.pop(0)
        for neighbour in list(graph.neighbors(m)):
            if neighbour not in visited:
                graph.nodes[neighbour]["weight"] = get_minimum_weight(
                    graph, neighbour)
                visited.append(neighbour)
                # if not neighbour in visited:
                queue_list.append(neighbour)
        # plot_graph(graph, g_pos, str(cnt)+'.'+m)
        cnt += 1
    shortlisted_state = []
    hash_weights = []
    for node in list(graph.nodes()):
        if graph.nodes[node]["weight"] <= max_depth:
            # if node in batch_statehash:
            shortlisted_state.append(node)
            hash_weights.append(graph.nodes[node]["weight"])

    shortlisted_state_hash_df = pd.DataFrame()
    shortlisted_state_hash_df["state_hash"] = shortlisted_state
    shortlisted_state_hash_df["weight"] = hash_weights
    return shortlisted_state_hash_df


def send_slack_message(url, message, logger):
    "Send a slack message to the specified URL."
    payload = '{"text": "%s" }' % message
    response = requests.post(url, data=payload)
    logger.info(response)
