"""Project DB access using the limited runtime role and a mounted secret file."""
import os
from pathlib import Path
import psycopg2


def connect_project_db():
    password = Path(os.environ["PROJECT_DB_PASSWORD_FILE"]).read_text().strip()
    if not password:
        raise ValueError("Project DB password file is empty")
    return psycopg2.connect(
        host=os.environ["PROJECT_DB_HOST"], dbname=os.environ["PROJECT_DB_NAME"],
        user=os.environ["PROJECT_DB_USER"], password=password, connect_timeout=10,
        application_name="ai-output-dq-pipeline",
    )
