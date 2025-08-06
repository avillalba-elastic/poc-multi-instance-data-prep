import json
import os
import shutil
import socket
import subprocess
import time
from typing import TypedDict

import ray
from loguru import logger


class ResourceConfig(TypedDict):
    """The hostnames of all the nodes."""

    current_host: str
    hosts: list[str]


class RayHelper:
    """Helper to start a Ray cluster in a Sagemaker Job."""

    def __init__(self, ray_port: str = "9339"):
        self.ray_port = ray_port

        self.resource_config = self.get_resource_config()
        self.master_host = self.resource_config["hosts"][0]
        self.n_hosts = len(self.resource_config["hosts"])

        self.is_local = self.master_host == "localhost" and self.n_hosts == 1

    @staticmethod
    def get_resource_config() -> ResourceConfig:
        """Get the hostnames of the nodes.

        It works in Sagemaker Processing and Training Jobs as well as
        in local mode.

        Returns:
            ResourceConfig: The current hostname and the list of
                the rest nodes hostnames.
        """

        processing_job_name = os.environ.get("PROCESSING_JOB_NAME", None)
        training_job_name = os.environ.get("TRAINING_JOB_NAME", None)

        if processing_job_name:
            logger.info("Running Ray in a Sagemaker Processing Job...")

            # In a Sagemaker Processing Job, the hostnames of the processing container
            # are stored automatically in `/opt/ml/config/resourceconfig.json`.
            # Reference: https://docs.aws.amazon.com/sagemaker/latest/dg/build-your-own-processing-container.html
            max_retries = 5
            retry_interval = 5  # seconds
            num_retries = 0
            resource_config_file = "/opt/ml/config/resourceconfig.json"
            host_info = None
            while num_retries < max_retries and not host_info:
                try:
                    with open(resource_config_file) as f:
                        host_info = json.load(f)
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    num_retries += 1
                    logger.info(f"{num_retries} attempt to open {resource_config_file} failed: {e}")
                    logger.info(f"Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)

            if not host_info:
                raise TimeoutError(f"Exceeded maximum time trying to load {resource_config_file}")

        elif training_job_name:
            logger.info("Running Ray in a Sagemaker Training Job...")

            # These environment variable are automatically set in Sagemaker Training Jobs
            current_host = os.environ.get("SM_CURRENT_HOST")
            hosts = os.environ.get("SM_HOSTS")

            if current_host and hosts:
                host_info = {"current_host": current_host, "hosts": json.loads(hosts)}
            else:
                raise ValueError("Missing `SM_CURRENT_HOST` or `SM_HOSTS` environment variables.")
        else:  # local mode
            logger.info("Running Ray in local mode...")
            host_info = {"current_host": "localhost", "hosts": ["localhost"]}

        logger.info(f"Hostnames: {host_info}")
        return host_info

    def _get_master_host_ip(self) -> str:
        """Resolves the IP of the master node.

        Raises:
            TimeoutError: If DNS resolution exceeds the timeout

        Returns:
            str: The IP of the master node
        """

        max_wait_time = 200  # seconds
        retry_interval = 1  # seconds
        deadline = time.time() + max_wait_time

        while time.time() < deadline:
            try:
                ip = socket.gethostbyname(self.master_host)
                return ip
            except socket.gaierror as e:
                logger.info(f"DNS resolution failed for {self.master_host}: {e}")
                time.sleep(retry_interval)

        raise TimeoutError(
            f"Exceeded {max_wait_time}s waiting for DNS resolution of {self.master_host}"
        )

    def start_ray(self) -> None:
        """Starts the Ray cluster.

        Raises:
            RuntimeError: If Ray is not found in PATH
        """

        if self.is_local:
            logger.info("Starting Ray in local mode")
            ray.init()  # by default, ray will use all the available resources in the local machine
            logger.info(ray.cluster_resources())
        else:
            ray_executable = shutil.which("ray")
            if ray_executable is None:
                raise RuntimeError("The 'ray' executable was not found in PATH")

            if self.resource_config["current_host"] == self.master_host:
                # Start the HEAD node in the master node
                output = subprocess.run(  # noqa: S603
                    [
                        ray_executable,
                        "start",
                        "--head",
                        "-vvv",
                        "--port",
                        self.ray_port,
                        "--include-dashboard",
                        "false",
                    ],
                    stdout=subprocess.PIPE,
                    check=True,
                )
                logger.info(output.stdout.decode("utf-8"))

                ray.init(address="auto", include_dashboard=False)
                self._wait_for_workers()
                logger.info("All workers present and accounted for")
                logger.info(ray.cluster_resources())
            else:
                # Connect the worker node to the Ray cluster
                master_ip = self._get_master_host_ip()
                time.sleep(10)
                subprocess.run(  # noqa: S603
                    [ray_executable, "start", f"--address={master_ip}:{self.ray_port}", "--block"],
                    stdout=subprocess.PIPE,
                    check=True,
                )

    def _wait_for_workers(self, timeout: int = 60) -> None:
        """Waits for the Ray workers to join the Ray cluster.

        Args:
            timeout (int, optional): The maximum time to wait, in seconds. Defaults to 60.

        Raises:
            Exception: If timeout is exceeded.
        """
        logger.info(f"Waiting {timeout} seconds for {self.n_hosts} nodes to join")

        while len(ray.nodes()) < self.n_hosts:
            logger.info(f"{len(ray.nodes())} connected to the cluster")
            time.sleep(5)
            timeout -= 5
            if timeout == 0:
                raise Exception("Max timeout for nodes to join exceeded")
