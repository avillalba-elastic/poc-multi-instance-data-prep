import os

import boto3
import psutil
from loguru import logger


def log_memory() -> None:  # noqa: D103
    process = psutil.Process(os.getpid())
    musage = process.memory_info().rss / 1024**2
    logger.info(f"Memory usage: {musage:.2f} MB")


def auth() -> None:  # noqa: D103
    ml_session = boto3.Session(profile_name="sagemaker", region_name="us-east-1")

    credentials = ml_session.get_credentials().get_frozen_credentials()
    os.environ["AWS_ACCESS_KEY_ID"] = credentials.access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
    os.environ["AWS_SESSION_TOKEN"] = credentials.token
