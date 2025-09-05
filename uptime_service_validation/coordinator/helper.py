"""This module contains various helper functions and classes for the
coordinator."""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
from typing import ByteString, Optional, List
import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import psycopg2
from psycopg2 import extras
import requests
from oauth2client.service_account import ServiceAccountCredentials
import gspread

from uptime_service_validation.coordinator.config import Config

ERROR = "Error: {0}"


@dataclass
class Submission:
    "Represents a submission to the network."

    submitted_at_date: str
    submitted_at: datetime
    submitter: str
    created_at: datetime
    block_hash: str
    remote_addr: str
    peer_id: str
    graphql_control_port: int
    built_with_commit_sha: str
    snark_work: Optional[ByteString] = None
    state_hash: Optional[str] = None
    parent: Optional[str] = None
    height: Optional[int] = None
    slot: Optional[int] = None
    validation_error: Optional[str] = None
    verified: Optional[bool] = None


class Timer:
    "This is a simple context manager to measure execution time."

    def __init__(self):
        self.start_time = None
        self.end_time = None

    @contextmanager
    def measure(self):
        """Measure execution time of a code bloc. Store results in start_time
        and end_time properties."""
        self.start_time = datetime.now()
        yield
        self.end_time = datetime.now()

    @property
    def duration(self):
        "Return the duration of the measured interval."
        return self.end_time - self.start_time


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
            start_time=self.end_time, interval=self.interval, bot_log_id=-bot_log_id
        )

    def split(self, parts_number):
        "Splits the batch time window into equal parts for parallel proecessing."
        diff = (self.end_time - self.start_time) / parts_number
        return (
            (self.start_time + diff * i, self.start_time + diff * (i + 1))
            for i in range(parts_number)
        )


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
            start_time=prev_batch_end, bot_log_id=bot_log_id, interval=interval
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
                result, columns=["parent_state_hash", "state_hash", "weight"]
            )
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
        self.logger.info("create_node_record  start ")
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
        self.logger.info("create_node_record  end ")
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
        temp_df = df[["parent_state_hash", "state_hash", "weight", "bot_log_id"]]
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
        self.logger.info(
            "updateScoreboard  start (score_till_time: %s, uptime_days: %s) ",
            score_till_time,
            uptime_days,
        )
        # update the scores
        # Note that points_summary table is updated by the database trigger
        # on every insert to the points table.
        # It holds one record per block producer per batch if they submitted any valid submissions within the batch.
        # Scores are calculated based on the points_summary table.
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
                where b.batch_start_epoch >= start_epoch and  b.batch_end_epoch <= end_epoch and b.files_processed > -1
              )
              , scores as (
                select p.node_id, count(p.bot_log_id) bp_points
                from points_summary p join bot_logs b on p.bot_log_id =b.id, epochs
                where b.batch_start_epoch >= start_epoch and b.batch_end_epoch <= end_epoch
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

    # tuples = [discord_id, block_producer_email, block_producer_key]
    def update_application_status(self, tuples, page_size=100):
        "Update the application status of the block producers."
        self.logger.info("update_application_status  start ")
        cursor = self.connection.cursor()
        try:
            sql = """update nodes set application_status = true, discord_id =%s, email_id =%s
                where block_producer_key= %s """
            extras.execute_batch(cursor, sql, tuples, page_size)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(ERROR.format(error))
            cursor.close()
            return -1
        finally:
            cursor.close()
            self.connection.commit()
        self.logger.info("update_application_status  end ")
        return 0

    def insert_submissions(self, submissions):
        """Insert a list of Submission objects into the submissions table."""
        self.logger.info(
            "insert_submissions  start (submissions: %s)", len(submissions)
        )
        insert_query = """
            INSERT INTO submissions (
                submitted_at_date, submitted_at, submitter, remote_addr, block_hash, 
                state_hash, parent, height, slot, validation_error, verified
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        values = [
            (
                submission.submitted_at_date,
                submission.submitted_at,
                submission.submitter,
                submission.remote_addr,
                submission.block_hash,
                submission.state_hash,
                submission.parent,
                submission.height,
                submission.slot,
                submission.validation_error,
                submission.verified,
            )
            for submission in submissions
        ]

        cursor = self.connection.cursor()
        try:
            extras.execute_batch(cursor, insert_query, values)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error inserting submissions: %s", error)
            cursor.close()
            return -1
        finally:
            cursor.close()
        self.logger.info("insert_submissions  end")
        return 0

    # return list of Submission objects
    def get_submissions(
        self, start_date: datetime, end_date: datetime
    ) -> Optional[List[Submission]]:
        "Get the submissions for a given submitter and time frame."
        query = """
            SET TIME ZONE 'UTC';
            SELECT 
                submitted_at_date, 
                submitted_at, 
                submitter, 
                created_at, 
                block_hash, 
                remote_addr, 
                peer_id, 
                graphql_control_port, 
                built_with_commit_sha, 
                state_hash, 
                parent, 
                height, 
                slot, 
                validation_error, 
                verified 
            FROM submissions 
            WHERE submitted_at >= %s 
            AND submitted_at < %s
            ORDER BY submitted_at DESC;
        """

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, (start_date, end_date))

            # print query with parameters
            # preview_query = cursor.mogrify(query, (start_date, end_date))
            # print(preview_query.decode("utf-8"))
            result = cursor.fetchall()
            # convert the result to a list of Submission objects
            submissions = [
                Submission(
                    submitted_at_date=row[0],
                    submitted_at=row[1],
                    submitter=row[2],
                    created_at=row[3],
                    block_hash=row[4],
                    remote_addr=row[5],
                    peer_id=row[6],
                    graphql_control_port=row[7],
                    built_with_commit_sha=row[8],
                    state_hash=row[9],
                    parent=row[10],
                    height=row[11],
                    slot=row[12],
                    validation_error=row[13],
                    verified=row[14],
                )
                for row in result
            ]
            return submissions
        except psycopg2.Error as e:
            self.logger.error("Database error: %s", e)
            return None


def get_contact_details_from_spreadsheet():
    "Get the contact details of the block producers from the Google spreadsheet."
    os.environ["PYTHONIOENCODING"] = "utf-8"
    spreadsheet_scope = Config.SPREADSHEET_SCOPE
    spreadsheet_name = Config.SPREADSHEET_NAME
    spreadsheet_credentials_json = Config.SPREADSHEET_CREDENTIALS_JSON
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        spreadsheet_credentials_json, spreadsheet_scope
    )
    client = gspread.authorize(creds)
    sheet = client.open(spreadsheet_name)
    sheet_instance = sheet.get_worksheet(0)
    records_data = sheet_instance.get_all_records(expected_headers=[])
    table_data = pd.DataFrame(records_data)
    spread_df = table_data.iloc[:, [2, 3, 4]]
    tuples = [tuple(x) for x in spread_df.to_numpy()]
    return tuples


def get_relations(df):
    "Extract parent-child relations between statehashes in a dataframe."
    return (
        (parent, child)
        for child, parent in df[["state_hash", "parent_state_hash"]].values
        if parent in df["state_hash"].values
    )


def find_new_values_to_insert(existing_values, new_values):
    "Find the new values to insert into the database."
    return (
        existing_values.merge(new_values, how="outer", indicator=True)
        .loc[lambda x: x["_merge"] == "right_only"]
        .drop("_merge", axis=1)
        .drop_duplicates()
    )


def filter_state_hash_percentage(df, p=0.05):
    "Filter statehashes by percentage of block producers who submitted them."
    state_hash_list = (
        df["state_hash"].value_counts().sort_values(ascending=False).index.to_list()
    )
    # get 5% number of blk in given batch
    total_unique_blk = df["block_producer_key"].nunique()
    percentage_result = round(total_unique_blk * p, 2)
    good_state_hash_list = list()
    for s in state_hash_list:
        blk_count = df[df["state_hash"] == s]["block_producer_key"].nunique()
        # check blk_count for state_hash submitted by blk least 5%
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
                graph.nodes[neighbour]["weight"] = get_minimum_weight(graph, neighbour)
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
