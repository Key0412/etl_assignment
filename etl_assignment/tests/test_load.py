from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pandas import DataFrame

from etl_assignment.general_config import get_config
from etl_assignment.logging_config import get_logger, setup_logging
from etl_assignment.steps.load import SaveCSVLocally, UploadToBucket

setup_logging()
logger = get_logger()
logger.debug("Testing Load Steps")

TEMP_DIR, CHUNK_SIZE = get_config()


class TestSaveCSVLocally:
    """Tests for SaveCSVLocally class."""

    @patch("etl_assignment.steps.load.Path.mkdir")
    @patch("etl_assignment.steps.load.DataFrame.to_csv")
    def test_run_step_success(
        self, mock_to_csv: MagicMock, mock_mkdir: MagicMock
    ) -> None:
        """Test the run_step method when it succeeds."""
        # Mock DataFrame
        df = DataFrame({"column1": [1, 2], "column2": [3, 4]})
        save_csv = SaveCSVLocally(data=df, file_path=f"{TEMP_DIR}/test.csv")

        # Run the method
        save_csv.run_step()

        # Verify that to_csv was called and step_result is set
        # fmt: off
        mock_to_csv.assert_called_once_with(
            Path(f"{TEMP_DIR}/test.csv"), index=False
        )  # noqa
        # fmt: on
        assert save_csv.step_result["file_path"] == f"{TEMP_DIR}/test.csv"

    @patch("etl_assignment.steps.load.Path.mkdir")
    @patch("etl_assignment.steps.load.DataFrame.to_csv")
    def test_run_step_failure(
        self, mock_to_csv: MagicMock, mock_mkdir: MagicMock
    ) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate an error in saving the CSV
        mock_to_csv.side_effect = Exception("Save error")
        df = DataFrame({"column1": [1, 2], "column2": [3, 4]})
        save_csv = SaveCSVLocally(data=df, file_path=f"{TEMP_DIR}/test.csv")

        with pytest.raises(Exception, match="Save error"):
            save_csv.run_step()


class TestUploadToBucket:
    """Tests for UploadToBucket class."""

    @patch("etl_assignment.steps.load.read_csv")
    @patch("etl_assignment.steps.load.DataFrame.to_csv")
    def test_run_step_success(
        self, mock_to_csv: MagicMock, mock_read_csv: MagicMock
    ) -> None:
        """Test the run_step method when it succeeds."""
        # Mock DataFrame
        df = DataFrame({"column1": [1, 2], "column2": [3, 4]})
        mock_read_csv.return_value = df
        storage_options = {
            "aws_access_key_id": "fake",
            "aws_secret_access_key": "fake_secret",
        }
        upload = UploadToBucket(
            file_path=f"{TEMP_DIR}/test.csv",
            bucket_address="s3://my-bucket",
            key="folder/test.csv",
            storage_options=storage_options,
        )

        # Run the method
        upload.run_step()

        # Verify that to_csv was called with the correct parameters
        mock_to_csv.assert_called_once_with(
            "s3://my-bucket/folder/test.csv",
            index=False,
            chunksize=CHUNK_SIZE,
            storage_options=storage_options,
        )
        # fmt: off
        assert (upload.step_result["bucket_path"] == "s3://my-bucket/folder/test.csv")  # noqa
        # fmt: on

    @patch("etl_assignment.steps.load.read_csv")
    @patch("etl_assignment.steps.load.DataFrame.to_csv")
    def test_run_step_failure(
        self, mock_to_csv: MagicMock, mock_read_csv: MagicMock
    ) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate an error in uploading the CSV
        mock_to_csv.side_effect = Exception("Upload error")
        df = DataFrame({"column1": [1, 2], "column2": [3, 4]})
        mock_read_csv.return_value = df
        storage_options = {
            "aws_access_key_id": "fake",
            "aws_secret_access_key": "fake_secret",
        }
        upload = UploadToBucket(
            file_path=f"{TEMP_DIR}/test.csv",
            bucket_address="s3://my-bucket",
            key="folder/test.csv",
            storage_options=storage_options,
        )

        with pytest.raises(Exception, match="Upload error"):
            upload.run_step()
