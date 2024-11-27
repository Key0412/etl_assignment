import pytest

from etl_assignment.steps.step import Step


class MockStep(Step):
    """Mock implementation of run_step."""

    def run_step(self) -> None:
        """Mock method to simulate step execution."""
        self.step_result = {"key": "value"}


@pytest.fixture  # type: ignore
def mock_step() -> MockStep:
    """Fixture to provide a mocked Step instance."""
    return MockStep()


class TestStep:
    """Unit tests for the Step class."""

    def test_step_initialization(self, mock_step: MockStep) -> None:
        """Test Step constructor initializes step_result dictionary."""
        assert isinstance(mock_step.step_result, dict)
        assert mock_step.step_result == {}

    def test_get_result(self, mock_step: MockStep) -> None:
        """Test that get_result() returns the step_result dictionary."""
        mock_step.step_result = {"key": "value"}
        result = mock_step.get_result()
        assert result == {"key": "value"}

    def test_run_step(self, mock_step: MockStep) -> None:
        """Test that run_step updates the step_result."""
        mock_step.run_step()
        result = mock_step.get_result()
        assert result == {"key": "value"}
