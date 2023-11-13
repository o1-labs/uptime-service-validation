import os
from cassandra.cluster import Cluster
from ssl import SSLContext, CERT_REQUIRED, PROTOCOL_TLS_CLIENT
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, ByteString


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
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
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
        boto_session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.aws_region,
        )
        return SigV4AuthProvider(boto_session)

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

    def get_blocks(self, limit=None, block_hash=None):
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

    def get_submissions(
        self,
        limit=None,
        submitted_at_date=None,
        submitted_at_start=None,
        submitted_at_end=None,
    ):
        base_query = f"SELECT submitted_at_date, submitted_at, submitter, created_at, block_hash, remote_addr, peer_id, snark_work, graphql_control_port, built_with_commit_sha, state_hash, parent, height, slot, validation_error FROM {self.aws_keyspace}.submissions"

        # For storing conditions and corresponding parameters
        conditions = []
        parameters = []

        # Adding conditions based on provided parameters
        if submitted_at_date:
            conditions.append("submitted_at_date = %s")
            parameters.append(submitted_at_date)
        if submitted_at_start:
            conditions.append("submitted_at >= %s")
            parameters.append(submitted_at_start)
        if submitted_at_end:
            conditions.append("submitted_at <= %s")
            parameters.append(submitted_at_end)

        # Constructing the final query
        if conditions:
            query = f"{base_query} WHERE {' AND '.join(conditions)}"
        else:
            query = base_query

        if limit is not None:
            query += f" LIMIT {limit}"

        # Executing the query with parameters
        print(query)
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
        print(all_blocks)

        print("Specific block:")
        specific_block = client.get_blocks(
            block_hash="YnmzigsYK5tiybax6LT9c2NyVxEPd6or5aqbQG5mZWnbMZrxGX"
        )
        print(specific_block[0].block_hash)

        print("Four blocks:")
        four_blocks = client.get_blocks(limit=4)
        for block in four_blocks:
            print(block.block_hash)

        print("All submissions:")
        submissions = client.get_submissions()
        for submission in submissions:
            print(submission.submitter, submission.submitted_at)

        print("Specific submissions:")
        submissions = client.get_submissions(
            submitted_at_date="2023-11-09",
            submitted_at_start="2023-11-09 14:30:00",
            submitted_at_end="2023-11-09 14:59:59",
        )
        for submission in submissions:
            print(submission.submitter, submission.submitted_at)
    finally:
        client.close()
