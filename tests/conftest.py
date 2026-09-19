import os


# Tests run in one process; deployed generator processes must use distinct IDs.
os.environ.setdefault("GATEWAY_SNOWFLAKE_WORKER_ID", "0")
