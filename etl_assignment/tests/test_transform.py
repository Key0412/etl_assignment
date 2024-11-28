from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pandas import DataFrame

# fmt: off
from etl_assignment.logging_config import get_logger, setup_logging
from etl_assignment.steps.transform import (GenerateColumnsFromFullNm,
                                            TransformXML)

# fmt: on
setup_logging()
logger = get_logger()
logger.debug("Testing Transform Steps")


class TestTransformXML:
    """Tests for TransformXML class."""

    @patch("etl_assignment.steps.transform.read_xml")
    def test_run_step_success(self, mock_read_xml: MagicMock) -> None:
        """Test the run_step method when it succeeds."""
        # Mock DataFrame
        df = DataFrame({"column1": [1], "column2": ["a_value"]})
        mock_read_xml.return_value = df

        # Initialize TransformXML instance
        transform_xml = TransformXML(
            file_path=Path("test.xml"),
            xpath="//item",
            namespaces={"ns": "namespace"},
            iterparse={"attr": ["value"]},
            column_map=lambda x: f"mapped_{x}",
        )

        # Run the method
        transform_xml.run_step()

        # Verify the result
        mock_read_xml.assert_called_once_with(
            Path("test.xml"),
            xpath="//item",
            namespaces={"ns": "namespace"},
            iterparse={"attr": ["value"]},
        )
        assert transform_xml.step_result["data"].columns[0] == "mapped_column1"

    @patch("etl_assignment.steps.transform.read_xml")
    def test_run_step_failure(self, mock_read_xml: MagicMock) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate an error in reading XML
        mock_read_xml.side_effect = Exception("XML parsing error")

        transform_xml = TransformXML(
            file_path=Path("test.xml"),
            xpath="//item",
            namespaces={"ns": "namespace"},
            iterparse={"attr": ["value"]},
        )

        with pytest.raises(Exception, match="XML parsing error"):
            transform_xml.run_step()


class TestGenerateColumnsFromFullNm:
    """Tests for GenerateColumnsFromFullNm class."""

    def test_run_step_success(self) -> None:
        """Test the run_step method when it succeeds."""
        # Mock DataFrame
        df = DataFrame({"FinInstrmGnlAttrbts.FullNm": ["apple", "banana"]})

        # Initialize GenerateColumnsFromFullNm instance
        generate_columns = GenerateColumnsFromFullNm(data=df)

        # Run the method
        generate_columns.run_step()

        # Verify the generated columns
        assert "a_count" in generate_columns.step_result["data"].columns
        assert "contains_a" in generate_columns.step_result["data"].columns
        # fmt: off
        assert generate_columns.step_result["data"]["a_count"].tolist() == [
            1,
            3,
        ]
        assert generate_columns.step_result["data"]["contains_a"].tolist() == [
            "YES",
            "YES",
        ]
        # fmt: on

    def test_run_step_failure(self) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate a DataFrame without the required column
        df = DataFrame({"WrongColumn": ["apple", "banana"]})
        generate_columns = GenerateColumnsFromFullNm(data=df)

        with pytest.raises(KeyError):
            generate_columns.run_step()
