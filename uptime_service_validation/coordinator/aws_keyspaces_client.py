import os
import boto3
from cassandra.cluster import Cluster
from ssl import SSLContext, CERT_REQUIRED, PROTOCOL_TLS_CLIENT
from cassandra.auth import PlainTextAuthProvider
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, ByteString, List

import pandas as pd


@dataclass
class Submission:
    submitted_at_date: str
    submitted_at: datetime
    submitter: str
    created_at: datetime
    block_hash: str
    remote_addr: str
    peer_id: str
    snark_work: ByteString
    graphql_control_port: int
    built_with_commit_sha: str
    state_hash: Optional[str] = None
    parent: Optional[str] = None
    height: Optional[int] = None
    slot: Optional[int] = None
    validation_error: Optional[str] = None


@dataclass
class Block:
    block_hash: str
    raw_block: ByteString


class AWSKeyspacesClient:
    def __init__(self):
        # Load environment variables
        self.aws_keyspace = os.environ.get("AWS_KEYSPACE")
        self.aws_region = os.environ.get("AWS_REGION")
        self.cassandra_user = os.environ.get("CASSANDRA_USER")
        self.cassandra_pass = os.environ.get("CASSANDRA_PASS")
        self.aws_ssl_certificate_path = os.environ.get("AWS_SSL_CERTIFICATE_PATH")

        self.ssl_context = self._create_ssl_context()
        self.auth_provider = self._create_auth_provider()
        self.cluster = self._create_cluster()

    def _create_ssl_context(self):
        ssl_context = SSLContext(PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(self.aws_ssl_certificate_path)
        ssl_context.verify_mode = CERT_REQUIRED
        ssl_context.check_hostname = False
        return ssl_context

    def _create_auth_provider(self):
        return PlainTextAuthProvider(
            username=self.cassandra_user, password=self.cassandra_pass
        )

    def _create_cluster(self):
        return Cluster(
            ["cassandra." + self.aws_region + ".amazonaws.com"],
            ssl_context=self.ssl_context,
            auth_provider=self.auth_provider,
            port=9142,
        )

    def connect(self):
        self.session = self.cluster.connect()

    def execute_query(self, query, parameters=None):
        if parameters:
            return self.session.execute(query, parameters)
        else:
            return self.session.execute(query)

    def get_blocks(
        self, limit: Optional[int] = None, block_hash: Optional[str] = None
    ) -> List[Block]:
        base_query = f"SELECT block_hash, raw_block FROM {self.aws_keyspace}.blocks"

        if block_hash:
            # Fetch block by specific hash
            query = f"{base_query} WHERE block_hash = %s"
            results = self.execute_query(query, (block_hash,))
        elif limit is not None:
            # Fetch a limited number of blocks
            query = f"{base_query} LIMIT {limit}"
            results = self.execute_query(query)
        else:
            # Fetch all blocks
            results = self.execute_query(base_query)

        blocks = [
            Block(block_hash=row.block_hash, raw_block=row.raw_block) for row in results
        ]
        return blocks

    # get list of submitted_at_date in the form of [YYYY-MM-DD]
    # submitted_at_date is needed, along with start_date and end_date, as input to get list of submissions from Cassandra AWS Keyspace
    @staticmethod
    def get_submitted_at_date_list(
        start_date: datetime, end_date: datetime
    ) -> List[str]:
        submitted_at_date_start = start_date.date()
        submitted_at_date_end = end_date.date()
        if submitted_at_date_start == submitted_at_date_end:
            submitted_at_dates = [submitted_at_date_start.strftime("%Y-%m-%d")]
        else:
            submitted_at_dates = (
                pd.date_range(submitted_at_date_start, submitted_at_date_end)
                .map(lambda x: x.date().strftime("%Y-%m-%d"))
                .to_list()
            )
        return submitted_at_dates

    def get_submissions(
        self,
        limit: Optional[int] = None,
        submitted_at_start: Optional[datetime] = None,
        submitted_at_end: Optional[datetime] = None,
        start_inclusive: bool = True,
        end_inclusive: bool = False,
    ) -> List[Submission]:
        # you have to provide either both submitted_at_start and submitted_at_end or neither
        if (submitted_at_start and not submitted_at_end) or (
            not submitted_at_start and submitted_at_end
        ):
            raise ValueError(
                "You have to provide either both submitted_at_start and submitted_at_end or neither"
            )

        base_query = f"SELECT submitted_at_date, submitted_at, submitter, created_at, block_hash, remote_addr, peer_id, snark_work, graphql_control_port, built_with_commit_sha, state_hash, parent, height, slot, validation_error FROM {self.aws_keyspace}.submissions"

        # For storing conditions and corresponding parameters
        conditions = []
        parameters = []

        # Getting submitted_at_date list
        if submitted_at_start and submitted_at_end:
            submitted_at_date_list = self.get_submitted_at_date_list(
                submitted_at_start, submitted_at_end
            )

            if len(submitted_at_date_list) == 1:
                submitted_at_date = submitted_at_date_list[0]
            else:
                submitted_at_date = None
                submitted_at_dates = ",".join(
                    [
                        f"'{submitted_at_date}'"
                        for submitted_at_date in submitted_at_date_list
                    ]
                )
            # Adding conditions based on provided parameters
            if submitted_at_date:
                conditions.append("submitted_at_date = %s")
                parameters.append(submitted_at_date)
            elif submitted_at_dates:
                conditions.append(f"submitted_at_date IN ({submitted_at_dates})")
            if submitted_at_start:
                start_operator = ">=" if start_inclusive else ">"
                conditions.append(f"submitted_at {start_operator} %s")
                parameters.append(submitted_at_start)
            if submitted_at_end:
                end_operator = "<=" if end_inclusive else "<"
                conditions.append(f"submitted_at {end_operator} %s")
                parameters.append(submitted_at_end)

        # Constructing the final query
        if conditions:
            query = f"{base_query} WHERE {' AND '.join(conditions)}"
        else:
            query = base_query

        if limit is not None:
            query += f" LIMIT {limit}"

        # Executing the query with parameters
        results = self.execute_query(query, parameters)

        # Mapping results to Submission dataclass instances
        submissions = [
            Submission(
                submitted_at_date=row.submitted_at_date,
                submitted_at=row.submitted_at,
                submitter=row.submitter,
                created_at=row.created_at,
                block_hash=row.block_hash,
                remote_addr=row.remote_addr,
                peer_id=row.peer_id,
                snark_work=row.snark_work,
                graphql_control_port=row.graphql_control_port,
                built_with_commit_sha=row.built_with_commit_sha,
                state_hash=row.state_hash,
                parent=row.parent,
                height=row.height,
                slot=row.slot,
                validation_error=row.validation_error,
            )
            for row in results
        ]
        return submissions

    def close(self):
        self.cluster.shutdown()


# Usage Example
if __name__ == "__main__":
    client = AWSKeyspacesClient()
    try:
        client.connect()
        print("All blocks:")
        all_blocks = client.get_blocks()
        print("Number of blocks:", len(all_blocks))
        print()

        print("Specific block:")
        specific_block = client.get_blocks(
            block_hash="YnmzigsYK5tiybax6LT9c2NyVxEPd6or5aqbQG5mZWnbMZrxGX"
        )
        print(specific_block[0].block_hash)
        print()

        print("Four blocks:")
        four_blocks = client.get_blocks(limit=4)
        for block in four_blocks:
            print(block.block_hash)
        print()

        print("All submissions:")
        submissions = client.get_submissions()
        print("Number of submissions:", len(submissions))
        print()

        print("Specific submissions:")
        start = datetime(2023, 11, 9, 16, 2, 0)
        end = datetime(2023, 11, 14, 13, 26, 10)
        submissions = client.get_submissions(
            submitted_at_start=start,
            submitted_at_end=end,
            start_inclusive=True,
            end_inclusive=False,
        )
        for submission in submissions:
            print(submission.submitter, submission.submitted_at, submission.block_hash)
        print(
            "Number of submissions between '%s' and '%s': %s"
            % (start, end, len(submissions))
        )

    finally:
        client.close()
