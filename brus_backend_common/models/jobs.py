import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Generator

from brus_backend_common.helpers.configs import LOCAL_EXTENDED_EXTRA_CONF, OPTIONAL_SPARK_HIVE_JAR, SPARK_SESSION_JARS

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class _AbstractStrategy(ABC):
    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict | None:
        pass


class EmrServerlessStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "EMR_SERVERLESS"

    @abstractmethod
    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs: str) -> dict:
        # TODO: This will be implemented as we migrate, but added as a placeholder for now
        pass


class LocalStrategy(_AbstractStrategy):
    @property
    def name(self) -> str:
        return "LOCAL"

    @staticmethod
    @contextmanager
    def _get_spark_session() -> Generator["SparkSession", None, None]:
        from brus_backend_common.helpers.spark import configure_spark_session, get_active_spark_session

        extra_conf = {
            **LOCAL_EXTENDED_EXTRA_CONF,
            # Overwrite to allow more memory given this will process more data than test cases
            "spark.driver.memory": "2g",
            "spark.executor.memory": "2g",
        }
        spark = get_active_spark_session()
        spark_created_for_job = False
        if not spark:
            spark_created_for_job = True
            # Type Checkers struggle with **kwargs and there's still no consensus on how to resolve them
            spark = configure_spark_session(spark_context=spark, enable_hive_support=True, **extra_conf)  # type: ignore

        yield spark

        if spark_created_for_job:
            spark.stop()

    @staticmethod
    def _run_in_container(job_name: str, command_name: str, command_options: list[str]) -> str:
        import docker

        client = docker.from_env()
        try:
            template_container = client.containers.get("spark-submit")
        except docker.errors.NotFound:
            logger.exception(
                f"The 'spark-submit' container was not found. Please create this container first via the supported"
                " spark-submit docker compose workflow."
            )
            raise
        image_name = template_container.attrs["Config"]["Image"]
        image_exists = client.images.list(name=image_name)
        if not image_exists:
            msg = (
                f"The '{image_name}' image was not found. Please create this image first via the supported"
                " spark docker compose workflows."
            )
            logger.error(msg)
            raise RuntimeError(msg)
        volumes = template_container.attrs["Mounts"]
        environment_variables = template_container.attrs["Config"]["Env"]
        network = template_container.attrs["HostConfig"]["NetworkMode"]
        options_as_string = " ".join(command_options)
        container_name = f"spark-submit_{job_name}_{datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
        required_jars = ",".join([*SPARK_SESSION_JARS, OPTIONAL_SPARK_HIVE_JAR])
        client.containers.run(
            image_name,
            name=container_name,
            network=network,
            command=(
                'spark-submit --driver-memory "2g"'
                f" --packages {required_jars}"
                f" /project/manage.py {command_name} {options_as_string}"
            ),
            environment=[
                f"COMPONENT_NAME={command_name} {options_as_string}",
                *environment_variables,
            ],
            volumes=[f"{volume['Source']}:{volume['Destination']}" for volume in volumes],
        )
        return container_name

    def handle_start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict | None:
        run_as_container = kwargs.get("run_as_container", False)
        run_details = None
        try:
            if run_as_container:
                container_name = self._run_in_container(job_name, command_name, command_options)
                run_details = {"container_name": container_name}
            else:
                with self._get_spark_session():
                    pass
                    # TODO: Update to be based in scripts (that can be adapted for django commands)
                    # call_command(command_name, *command_options)
        except Exception:
            logger.exception(f"Failed on command: {command_name} {' '.join(command_options)}")
            raise
        return run_details


class SparkJobs:
    def __init__(self, strategy: _AbstractStrategy):
        self._strategy = strategy

    @property
    def strategy(self) -> _AbstractStrategy:
        return self._strategy

    @strategy.setter
    def strategy(self, strategy: _AbstractStrategy) -> None:
        self._strategy = strategy

    def start(self, job_name: str, command_name: str, command_options: list[str], **kwargs) -> dict | None:
        logger.info(f'Starting {job_name} on {self.strategy.name}: "{command_name} {" ".join(command_options)}"')
        run_details = self.strategy.handle_start(job_name, command_name, command_options, **kwargs)

        if run_details is None:
            msg = "Job completed successfully"
        else:
            msg = f"Job run successfully started; {run_details}"

        logger.info(msg)

        return run_details
