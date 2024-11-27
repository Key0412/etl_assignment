from typing import Any

from pandas import DataFrame

from etl_assignment.logging_config import get_logger, setup_logging
from etl_assignment.steps.step import Step

logger = get_logger()
setup_logging()


class Pipeline:
    def __init__(
        self, name: str, steps: list[tuple[type[Step], dict[str, Any]]]
    ) -> None:
        """Initializes the Pipeline class.
        Orchestrates the execution of multiple ETL steps.

        Args:
            name (str): The name of the pipeline.
            steps (list[tuple[type[Step], dict]]): A list of tuples.
                        Each tuple contains a step class and its parameters.
        """
        self.name = name
        self.pipeline_steps = steps
        self.step_result: dict[str, Any] = {}

    def run_pipeline(self) -> None:
        """Executes the pipeline by running each step sequentially.

        Logs the execution of each step and captures their results.
        If a step fails, the pipeline stops.
        """
        logger.info(f"Executing Pipeline: {self.name}")
        for step, params in self.pipeline_steps:
            params.update(self.step_result)
            logger.info(f"Executing step: {step.__name__}")
            logger.info(
                f"""params: {str(
                    {k: f'DataFrame ({v.shape}) cols: {list(v.columns)}'
                     if isinstance(v, DataFrame)
                     else v for k, v in params.items()})}"""
            )
            try:
                step_instance = step(**params)
                step_instance.run_step()
            except Exception as e:
                logger.error(f"{step.__name__} encountered an error: {e}")
                break
            result = step_instance.get_result()
            self.step_result = result
            logger.info(f"Step finished: {step.__name__}")
            logger.info(
                f"""result: {str(
                    {k: f'DataFrame ({v.shape}) cols: {list(v.columns)}'
                     if isinstance(v, DataFrame)
                     else v for k, v in result.items()})}"""
            )
        logger.info(f"Pipeline Finished: {self.name}")
