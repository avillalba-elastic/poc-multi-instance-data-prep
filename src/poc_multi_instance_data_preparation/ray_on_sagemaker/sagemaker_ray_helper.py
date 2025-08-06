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

        resource_config_file = "/opt/ml/config/resourceconfig.json"
        is_processing_job = os.path.exists(resource_config_file)

        is_training_job = bool(os.environ.get("SM_CURRENT_HOST"))

        if is_processing_job:
            logger.info("Running Ray in a Sagemaker Processing Job...")

            # In a Sagemaker Processing Job, the hostnames of the processing container
            # are stored automatically in `/opt/ml/config/resourceconfig.json`.
            # Reference: https://docs.aws.amazon.com/sagemaker/latest/dg/build-your-own-processing-container.html
            max_retries = 5
            retry_interval = 5  # seconds
            num_retries = 0

            resource_config = None
            while num_retries < max_retries and not resource_config:
                try:
                    with open(resource_config_file) as f:
                        resource_config = json.load(f)
                        host_info = ResourceConfig(
                            current_host=resource_config["current_host"],
                            hosts=resource_config["hosts"],
                        )
                except (json.JSONDecodeError, FileNotFoundError) as e:
                    num_retries += 1
                    logger.info(f"{num_retries} attempt to open {resource_config_file} failed: {e}")
                    logger.info(f"Retrying in {retry_interval} seconds...")
                    time.sleep(retry_interval)

            if not host_info:
                raise TimeoutError(f"Exceeded maximum time trying to load {resource_config_file}")

        elif is_training_job:
            logger.info("Running Ray in a Sagemaker Training Job...")

            # These environment variable are automatically set in Sagemaker Training Jobs
            current_host = os.environ.get("SM_CURRENT_HOST")
            hosts = os.environ.get("SM_HOSTS")

            if current_host and hosts:
                host_info = ResourceConfig(current_host=current_host, hosts=json.loads(hosts))
            else:
                raise ValueError("Missing `SM_CURRENT_HOST` or `SM_HOSTS` environment variables.")

            raise NotImplementedError("Ray with SageMaker Training Jobs has not been tested yet.")
        else:  # local mode
            logger.info("Running Ray in local mode...")
            host_info = ResourceConfig(current_host="localhost", hosts=["localhost"])

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
            logger.info("Local cluster successfully set up! Cluster resources: ")
            logger.info(ray.cluster_resources())
        else:
            ray_executable = shutil.which("ray")
            if ray_executable is None:
                raise RuntimeError("The 'ray' executable was not found in PATH")

            if self.resource_config["current_host"] == self.master_host:
                # Start the HEAD node in the master node
                logger.info(f"[HEAD] Starting Ray HEAD node at port {self.ray_port}")
                try:
                    output = subprocess.run(  # noqa: S603
                        [
                            ray_executable,
                            "start",
                            "--head",
                            "-vvv",
                            f"--port={self.ray_port}",
                            "--include-dashboard=false",
                        ],
                        capture_output=True,
                        check=True,
                        text=True,
                    )
                    logger.debug(f"[HEAD] `ray start --head` stdout: {output.stdout}")
                    logger.debug(f"[HEAD] `ray start --head` stderr: {output.stderr}")
                except subprocess.CalledProcessError as e:
                    logger.error(f"[HEAD] Ray failed to start (return code: {e.returncode})")
                    logger.error(f"[HEAD] stdout:\n{e.stdout}")
                    logger.error(f"[HEAD] stderr:\n{e.stderr}")
                    raise e

                logger.info("[HEAD] HEAD Ray node successfully started!")

                ray.init(address="auto", include_dashboard=False)
                logger.info("[HEAD] HEAD Ray node successfully connected to Ray cluster!")

                logger.info("[HEAD] Waiting for the WORKERs to join the Ray cluster...")
                self._wait_for_workers()
                logger.info("[HEAD] All WORKERs present and accounted for")

                logger.info(
                    "[HEAD] Ray cluster created and connection established! Cluster Resources: "
                )
                logger.info(ray.cluster_resources())
            else:
                # Connect the worker node to the Ray cluster
                master_ip = self._get_master_host_ip()
                time.sleep(10)  # HEAD node needs to start first.
                logger.info(
                    f"Connecting WORKER node to HEAD node at {master_ip}:{self.ray_port}..."
                )
                output = subprocess.run(  # noqa: S603
                    [ray_executable, "start", f"--address={master_ip}:{self.ray_port}", "--block"],
                    capture_output=True,
                    check=False,  # this process will end up with an exit code != 0
                    text=True,
                )
                logger.debug(
                    f"[WORKER] `ray start --address={master_ip}:{self.ray_port}` return code: {output.returncode}"  # noqa: E501
                )
                logger.debug(
                    f"[WORKER] `ray start --address={master_ip}:{self.ray_port}` stdout: {output.stdout}"  # noqa: E501
                )
                logger.debug(
                    f"[WORKER] `ray start --address={master_ip}:{self.ray_port}` stderr: {output.stderr}"  # noqa: E501
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
