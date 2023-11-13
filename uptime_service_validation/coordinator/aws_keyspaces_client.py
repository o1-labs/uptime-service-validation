import os
from cassandra.cluster import Cluster
from ssl import SSLContext, CERT_REQUIRED, PROTOCOL_TLS_CLIENT
import boto3
from cassandra_sigv4.auth import SigV4AuthProvider


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

    def execute_query(self, query):
        return self.session.execute(query)

    def close(self):
        self.cluster.shutdown()


# Usage Example
if __name__ == "__main__":
    client = AWSKeyspacesClient()
    try:
        client.connect()
        result = client.execute_query(
            "select * from " + client.aws_keyspace + ".blocks limit 1"
        )
        print(result.one())
    finally:
        client.close()
