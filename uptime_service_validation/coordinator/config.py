import os


class Config:
    """
    Configuration settings for the application.
    """

    # Runtime
    RETRY_COUNT = int(os.environ["RETRY_COUNT"])
    SURVEY_INTERVAL_MINUTES = int(os.environ["SURVEY_INTERVAL_MINUTES"])
    MINI_BATCH_NUMBER = int(os.environ["MINI_BATCH_NUMBER"])
    UPTIME_DAYS_FOR_SCORE = int(os.environ["UPTIME_DAYS_FOR_SCORE"])

    # Stateless Verifier
    WORKER_IMAGE = os.environ["WORKER_IMAGE"]
    WORKER_TAG = os.environ["WORKER_TAG"]
    NO_CHECKS = os.environ.get("NO_CHECKS")

    # Slack Alerts
    WEBHOOK_URL = os.environ.get("WEBHOOK_URL")
    ALARM_ZK_LOWER_LIMIT_SEC = os.environ.get("ALARM_ZK_LOWER_LIMIT_SEC")
    ALARM_ZK_UPPER_LIMIT_SEC = os.environ.get("ALARM_ZK_UPPER_LIMIT_SEC")

    # Submission Storage
    VALID_STORAGE_OPTIONS = ["CASSANDRA", "POSTGRES"]
    SUBMISSION_STORAGE = os.getenv("SUBMISSION_STORAGE", "POSTGRES").upper()

    # Postgres
    POSTGRES_HOST = os.environ["POSTGRES_HOST"]
    POSTGRES_DB = os.environ["POSTGRES_DB"]
    POSTGRES_USER = os.environ["POSTGRES_USER"]
    POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
    POSTGRES_PORT = os.environ["POSTGRES_PORT"]

    # Cassandra / Keyspaces
    AWS_KEYSPACE = os.environ.get("AWS_KEYSPACE")
    CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST")
    CASSANDRA_PORT = os.environ.get("CASSANDRA_PORT")
    CASSANDRA_USERNAME = os.environ.get("CASSANDRA_USERNAME")
    CASSANDRA_PASSWORD = os.environ.get("CASSANDRA_PASSWORD")
    # if AWS_ROLE_ARN, AWS_ROLE_SESSION_NAME and AWS_WEB_IDENTITY_TOKEN_FILE are set,
    # we are using AWS STS to assume a role and get temporary credentials
    # if they are not set, we are using AWS IAM user credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    AWS_ROLE_ARN = os.environ.get("AWS_ROLE_ARN")
    AWS_ROLE_SESSION_NAME = os.environ.get("AWS_ROLE_SESSION_NAME")
    AWS_WEB_IDENTITY_TOKEN_FILE = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
    AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
    SSL_CERTFILE = os.environ.get("SSL_CERTFILE")

    # Application status
    IGNORE_APPLICATION_STATUS = os.environ.get("IGNORE_APPLICATION_STATUS")
    SPREADSHEET_NAME = str(os.environ.get("SPREADSHEET_NAME")).strip()
    SPREADSHEET_ID = str(os.environ.get("SPREADSHEET_ID")).strip()
    SPREADSHEET_SCOPE = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    # Test environment
    TEST_ENV = os.environ.get("TEST_ENV")

    def is_test_environment():
        """
        Checks if the application is running in a test environment.

        :return: True if the application is running in a test environment.
        """
        return bool_env_var_set("TEST_ENV")

    def no_checks():
        """
        Checks if the stateless_verifier should be run with --no-checks.

        :return: True if the application is running in a test environment.
        """
        return bool_env_var_set("NO_CHECKS")

    def ignore_application_status():
        """
        Checks if the application should ignore the application status.

        :return: True if the application should ignore the application status.
        """
        return bool_env_var_set("IGNORE_APPLICATION_STATUS")


def bool_env_var_set(env_var_name):
    """
    Checks if an environment variable is set and is set to a truthy value.

    :param env_var_name: The name of the environment variable.
    :return: True if the environment variable is set and is set to a truthy value.
    """
    env_var = os.environ.get(env_var_name)
    return env_var is not None and env_var.lower() in ["true", "1"]
