import os

# required environment variables
os.environ["RETRY_COUNT"] = "3"
os.environ["SURVEY_INTERVAL_MINUTES"] = "3"
os.environ["MINI_BATCH_NUMBER"] = "3"
os.environ["UPTIME_DAYS_FOR_SCORE"] = "3"
os.environ["WORKER_IMAGE"] = "test_image"
os.environ["WORKER_TAG"] = "test_tag"
os.environ["POSTGRES_HOST"] = "test_host"
os.environ["POSTGRES_DB"] = "test_db"
os.environ["POSTGRES_USER"] = "test_user"
os.environ["POSTGRES_PASSWORD"] = "test_password"
os.environ["POSTGRES_PORT"] = "5432"
