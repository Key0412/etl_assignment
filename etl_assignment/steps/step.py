from abc import ABC, abstractmethod
from typing import Any


class Step(ABC):
    def __init__(self) -> None:
        """Abstract base class for pipeline steps.

        This class defines the structure for pipeline steps, requiring each
        step to implement the `run_step` method.
        The `step_result` is a dictionary that holds the output of the step's
        execution.
        """
        self.step_result: dict[str, Any] = {}

    @abstractmethod
    def run_step(self) -> None:
        """Abstract method to execute the logic for the pipeline step.

        This method should be overridden in the subclass.
        """
        pass

    def get_result(self) -> dict[str, Any]:
        """Retrieves the result of the pipeline step.

        Returns:
            dict: The result of the pipeline step execution, stored in the
            `step_result`.
        """
        return self.step_result
