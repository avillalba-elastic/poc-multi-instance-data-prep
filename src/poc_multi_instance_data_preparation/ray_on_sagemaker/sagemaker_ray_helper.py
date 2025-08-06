import json
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
        self.ray_port = ray_port

        self.resource_config = self.get_resource_config()
        self.master_host = self.resource_config["hosts"][0]
        self.n_hosts = len(self.resource_config["hosts"])

    @staticmethod
    def get_resource_config() -> dict[str, str | list[str]]:
        """Get the names of the Sagemaker nodes.

        Returns:
            dict[str, str | list[str]]: The current hostname and the list of
                the rest hostnames of the Sagemaker nodes.
        """

        current_host = os.environ.get("SM_CURRENT_HOST")
        hosts = os.environ.get("SM_HOSTS")

        if current_host and hosts:
            host_info = {"current_host": current_host, "hosts": json.loads(hosts)}
        else:
            raise ValueError("Missing `SM_CURRENT_HOST` or `SM_HOSTS` environment variables.")

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
        master_ip = self._get_master_host_ip()

        ray_executable = shutil.which("ray")
        if ray_executable is None:
            raise RuntimeError("The 'ray' executable was not found in PATH")

        if self.resource_config["current_host"] == self.master_host:
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
