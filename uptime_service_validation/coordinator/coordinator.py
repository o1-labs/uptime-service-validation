"""The coordinator script of the uptime service. Its job is to manage
the validator processes, distribute work them and, when they're done,
collect their results, compute scores for the delegation program and
put the results in the database."""

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import logging
import os
import sys
from time import sleep

from dotenv import load_dotenv
import pandas as pd
import psycopg2
from uptime_service_validation.coordinator.helper import (
    DB,
    Timer,
    get_relations,
    find_new_values_to_insert,
    filter_state_hash_percentage,
    create_graph,
    apply_weights,
    bfs,
    send_slack_message,
)
from uptime_service_validation.coordinator.server import (
    bool_env_var_set,
    setUpValidatorPods,
    setUpValidatorProcesses,
)
from uptime_service_validation.coordinator.aws_keyspaces_client import (
    AWSKeyspacesClient,
)

# Add project root to python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)


class State:
    """The state aggregates all the data that remains constant while processing
    a single batch, but changes between batches. It also takes care of valid
    state transitions. It should likely be split into smaller chunks, but at
    this stage it's sufficient."""

    def __init__(self, batch):
        self.batch = batch
        self.current_timestamp = datetime.now(timezone.utc)
        self.retrials_left = os.environ["RETRY_COUNT"]
        self.interval = int(os.environ["SURVEY_INTERVAL_MINUTES"])
        self.loop_count = 0
        self.stop = False

    def wait_until_batch_ends(self):
        "If the time window if the current batch is not yet over, sleep until it is."
        if self.batch.end_time > self.current_timestamp:
            delta = timedelta(minutes=2)
            sleep_interval = (self.batch.end_time - self.current_timestamp) + delta
            time_until = self.current_timestamp + sleep_interval
            logging.info(
                "All submissions are processed till date. "
                "Will wait %s (until %s) before starting next batch...",
                sleep_interval,
                time_until,
            )
            sleep(sleep_interval.total_seconds())
            self.__update_timestamp()

    def advance_to_next_batch(self, next_bot_log_id):
        """Update the state so that it describes the next batch in line;
        transitioning the state to the next loop pass."""
        self.retrials_left = os.environ["RETRY_COUNT"]
        self.batch = self.batch.next(next_bot_log_id)
        self.__warn_if_work_took_longer_then_expected()
        self.__next_loop()
        self.__update_timestamp()

    def retry_batch(self):
        "Transition the state for retrial of the current (failed) batch."
        if self.retrials_left > 0:
            self.retrials_left -= 1
            logging.error("Error in processing, retrying the batch...")
        logging.error("Error in processing, retry count exceeded... Exitting!")
        self.stop = True
        self.__warn_if_work_took_longer_then_expected()
        self.__next_loop()
        self.__update_timestamp()

    def __update_timestamp(self):
        self.current_timestamp = datetime.now(timezone.utc)

    def __next_loop(self):
        self.loop_count += 1
        logging.info("Processed it loop count : %s.", self.loop_count)

    def __warn_if_work_took_longer_then_expected(self):
        if self.batch.start_time >= self.current_timestamp:
            logging.warning(
                "It seems that batch processing took a bit too long than \
                expected as prev_batch_end: %s >= cur_timestamp: %s... \
                progressing to the next batch anyway...",
                self.batch.start_time,
                self.current_timestamp,
            )


def load_submissions(time_intervals):
    """Load submissions from Cassandra and return them as a DataFrame."""
    submissions = []
    submissions_verified = []
    cassandra = AWSKeyspacesClient()

    try:
        cassandra.connect()
        for time_interval in time_intervals:
            submissions.extend(
                cassandra.get_submissions(
                    submitted_at_start=time_interval[0],
                    submitted_at_end=time_interval[1],
                    start_inclusive=True,
                    end_inclusive=False,
                )
            )
    except Exception as e:
        logging.error("Error in loading submissions: %s", e)
        return pd.DataFrame([])
    finally:
        cassandra.close()

    # for further processing
    # we use only submissions verified = True and validation_error = None or ""
    for submission in submissions:
        if submission.verified and (
            submission.validation_error is None or submission.validation_error == ""
        ):
            submissions_verified.append(submission)

    all_submissions_count = len(submissions)
    submissions_to_process_count = len(submissions_verified)
    logging.info("number of all submissions: %s", all_submissions_count)
    logging.info("number of submissions to process: %s", submissions_to_process_count)
    if submissions_to_process_count < all_submissions_count:
        logging.warning(
            "some submissions were not processed, because they were not \
            verified or had validation errors"
        )
    return pd.DataFrame([asdict(submission) for submission in submissions_verified])


def process_statehash_df(db, batch, state_hash_df, verification_time):
    """Process the state hash dataframe and return the master dataframe."""
    all_files_count = state_hash_df.shape[0]
    master_df = pd.DataFrame()
    master_df["state_hash"] = state_hash_df["state_hash"]
    master_df["blockchain_height"] = state_hash_df["height"]
    master_df["slot"] = pd.to_numeric(state_hash_df["slot"])
    master_df["parent_state_hash"] = state_hash_df["parent"]
    master_df["submitter"] = state_hash_df["submitter"]
    master_df["file_updated"] = state_hash_df["submitted_at"]
    master_df["file_name"] = (
        state_hash_df["submitted_at"].astype(str)
        + "-"
        + state_hash_df["submitter"].astype(str)
    )  # Perhaps this should be changed? Filename makes less sense now.
    master_df["blockchain_epoch"] = state_hash_df["created_at"].apply(
        lambda row: int(row.timestamp() * 1000)
    )

    state_hash = pd.unique(
        master_df[["state_hash", "parent_state_hash"]].values.ravel("k")
    )
    existing_state_df = db.get_statehash_df()
    existing_nodes = db.get_existing_nodes()
    state_hash_to_insert = find_new_values_to_insert(
        existing_state_df, pd.DataFrame(state_hash, columns=["statehash"])
    )
    if not state_hash_to_insert.empty:
        db.create_statehash(state_hash_to_insert)

    nodes_in_cur_batch = pd.DataFrame(
        master_df["submitter"].unique(), columns=["block_producer_key"]
    )
    node_to_insert = find_new_values_to_insert(existing_nodes, nodes_in_cur_batch)

    if not node_to_insert.empty:
        node_to_insert["updated_at"] = datetime.now(timezone.utc)
        db.create_node_record(node_to_insert, 100)

    master_df.rename(
        inplace=True,
        columns={
            "file_updated": "file_timestamps",
            "submitter": "block_producer_key",
        },
    )

    relation_df, p_selected_node_df = db.get_previous_statehash(batch.bot_log_id)
    p_map = list(get_relations(relation_df))
    c_selected_node = filter_state_hash_percentage(master_df)
    batch_graph = create_graph(master_df, p_selected_node_df, c_selected_node, p_map)
    weighted_graph = apply_weights(
        batch_graph=batch_graph,
        c_selected_node=c_selected_node,
        p_selected_node=p_selected_node_df,
    )

    queue_list = list(p_selected_node_df["state_hash"].values) + c_selected_node

    batch_state_hash = list(master_df["state_hash"].unique())

    shortlisted_state_hash_df = bfs(
        graph=weighted_graph,
        queue_list=queue_list,
        node=queue_list[0],
        # batch_statehash=batch_state_hash, (this used to be here in old code,
        # but it's not used anywhere inside the function)
    )
    point_record_df = master_df[
        master_df["state_hash"].isin(shortlisted_state_hash_df["state_hash"].values)
    ]

    for index, row in shortlisted_state_hash_df.iterrows():
        if not row["state_hash"] in batch_state_hash:
            shortlisted_state_hash_df.drop(index, inplace=True, axis=0)
    p_selected_node_df = shortlisted_state_hash_df.copy()
    parent_hash = []
    for s in shortlisted_state_hash_df["state_hash"].values:
        p_hash = master_df[master_df["state_hash"] == s]["parent_state_hash"].values[0]
        parent_hash.append(p_hash)
    shortlisted_state_hash_df["parent_state_hash"] = parent_hash

    p_map = list(
        get_relations(shortlisted_state_hash_df[["parent_state_hash", "state_hash"]])
    )
    if not point_record_df.empty:
        file_timestamp = master_df.iloc[-1]["file_timestamps"]
    else:
        file_timestamp = batch.end_time
        logging.info(
            "empty point record for start epoch %s end epoch %s",
            batch.start_time.timestamp(),
            batch.end_time.timestamp(),
        )

    values = (
        all_files_count,
        file_timestamp,
        batch.start_time.timestamp(),
        batch.end_time.timestamp(),
        verification_time.total_seconds(),
    )
    bot_log_id = db.create_bot_log(values)

    shortlisted_state_hash_df["bot_log_id"] = bot_log_id
    db.insert_statehash_results(shortlisted_state_hash_df)

    if not point_record_df.empty:
        point_record_df.loc[:, "amount"] = 1
        point_record_df.loc[:, "created_at"] = datetime.now(timezone.utc)
        point_record_df.loc[:, "bot_log_id"] = bot_log_id
        point_record_df = point_record_df[
            [
                "file_name",
                "file_timestamps",
                "blockchain_epoch",
                "block_producer_key",
                "blockchain_height",
                "amount",
                "created_at",
                "bot_log_id",
                "state_hash",
            ]
        ]
        db.create_point_record(point_record_df)
    return bot_log_id


def process(db, state):
    """Perform a signle iteration of the coordinator loop, processing exactly
    one batch of submissions. Launch verifiers to process submissions, then
    compute scores and store them in the database."""
    logging.info(
        "iteration start at: %s, cur_timestamp: %s",
        state.batch.start_time,
        state.current_timestamp,
    )
    logging.info(
        "running for batch: %s - %s.", state.batch.start_time, state.batch.end_time
    )

    # sleep until batch ends, update the state accordingly, then continue.
    state.wait_until_batch_ends()
    time_intervals = list(state.batch.split(int(os.environ["MINI_BATCH_NUMBER"])))

    worker_image = os.environ["WORKER_IMAGE"]
    worker_tag = os.environ["WORKER_TAG"]
    timer = Timer()
    if bool_env_var_set("TEST_ENV"):
        logging.info("running in test environment")
        with timer.measure():
            setUpValidatorProcesses(time_intervals, logging, worker_image, worker_tag)
    else:
        with timer.measure():
            setUpValidatorPods(time_intervals, logging, worker_image, worker_tag)

    logging.info(
        "reading ZKValidator results from a db between the time range: %s - %s",
        state.batch.start_time,
        state.batch.end_time,
    )

    logging.info("ZKValidator results read from a db in %s.", timer.duration)
    webhook_url = os.environ.get("WEBHOOK_URL")
    if webhook_url is not None:
        if timer.duration < float(os.environ["ALARM_ZK_LOWER_LIMIT_SEC"]):
            send_slack_message(
                webhook_url,
                f"ZkApp Validation took {timer.duration} seconds, which is too quick",
                logging,
            )
        if timer.duration > float(os.environ["ALARM_ZK_UPPER_LIMIT_SEC"]):
            send_slack_message(
                webhook_url,
                f"ZkApp Validation took {timer.duration}, which is too long",
                logging,
            )

    state_hash_df = load_submissions(time_intervals)
    if not state_hash_df.empty:
        try:
            bot_log_id = process_statehash_df(
                db, state.batch, state_hash_df, timer.duration
            )
            db.connection.commit()
        except Exception as error:
            db.connection.rollback()
            logging.error("ERROR: %s", error)
            state.retry_batch()
            return
    else:
        # new bot log id hasn't been created, so proceed with the old one
        bot_log_id = state.batch.bot_log_id
        logging.info("Finished processing data from table.")
    try:
        db.update_scoreboard(
            state.batch.end_time,
            int(os.environ["UPTIME_DAYS_FOR_SCORE"]),
        )
    except Exception as error:
        db.connection.rollback()
        logging.error("ERROR: %s", error)
    else:
        db.connection.commit()
    state.advance_to_next_batch(bot_log_id)


def main():
    "The entrypoint to the program."
    load_dotenv()

    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    connection = psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )

    interval = int(os.environ["SURVEY_INTERVAL_MINUTES"])
    db = DB(connection, logging)
    batch = db.get_batch_timings(timedelta(minutes=interval))
    state = State(batch)
    while not state.stop:
        process(db, state)


if __name__ == "__main__":
    main()
