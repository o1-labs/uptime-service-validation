import os

# Default values for tests that touch Config attributes which still have
# `.get(..., default)` semantics in production but where a specific value is
# convenient. POSTGRES_* are no longer set here — Config reads them lazily
# via os.environ.get and validates presence at startup, so tests that don't
# exercise the Postgres path don't need them.
os.environ.setdefault("RETRY_COUNT", "3")
os.environ.setdefault("SURVEY_INTERVAL_MINUTES", "3")
os.environ.setdefault("MINI_BATCH_NUMBER", "3")
os.environ.setdefault("UPTIME_DAYS_FOR_SCORE", "3")
os.environ.setdefault("WORKER_IMAGE", "test_image")
os.environ.setdefault("WORKER_TAG", "test_tag")
