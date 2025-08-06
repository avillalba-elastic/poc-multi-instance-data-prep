import os
import shutil
import socket
import subprocess
import time

import ray
from loguru import logger


class RayHelper:
    """Helper to start a Ray cluster in a Sagemaker Job."""

    def __init__(self, ray_port: str = "9339"):
        self.hostname = socket.gethostname()
        self.ray_port = ray_port

        # In Sagemaker, N_HOSTS must be set via env variable
        self.is_local = "N_HOSTS" not in os.environ

        if self.is_local:
            logger.warning("`N_HOSTS` is not set as environment variable.Assuming local mode...")
            self.master_host = "localhost"
            self.n_hosts = 1
        else:
            # By default, the HEAD node will be the node 0
            cluster_prefix = self.hostname.rsplit("-", 1)[0]
            self.master_host = f"{cluster_prefix}-0"
            self.n_hosts = int(os.environ["N_HOSTS"])

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

        logger.info("Starting Ray cluster...")
        logger.info(f"Cluster Master host: {self.master_host}")
        logger.info(f"Number of nodes in the cluster: {self.n_hosts}")

        if self.is_local:
            logger.info("Starting Ray in local mode")
            ray.init()  # by default, ray will use all the available resources in the local machine
            logger.info(ray.cluster_resources())
        else:
            ray_executable = shutil.which("ray")
            if ray_executable is None:
                raise RuntimeError("The 'ray' executable was not found in PATH")

            if self.hostname == self.master_host:
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
