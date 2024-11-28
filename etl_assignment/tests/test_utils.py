from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests  # type: ignore

from etl_assignment.general_config import get_config
from etl_assignment.logging_config import get_logger, setup_logging
from etl_assignment.steps.utils import Utils

setup_logging()
logger = get_logger()
logger.debug("Testing Utils")

TEMP_DIR, CHUNK_SIZE = get_config()


class TestUtils:
    """Unit tests for the Utils class."""

    @patch("etl_assignment.steps.utils.ZipFile")
    def test_unzip_file_success(self, MockZipFile: MagicMock) -> None:
        """Test unzip_file method when it succeeds."""
        # Create a mock for the ZipFile context manager
        mock_zip = MagicMock()
        MockZipFile.return_value.__enter__.return_value = mock_zip

        file_path = Path("/path/to/file.zip")
        Utils.unzip_file(file_path)

        # Check that ZipFile was called with the correct file path
        MockZipFile.assert_called_once_with(file_path, "r")

        # Ensure extractall() was called once
        mock_zip.extractall.assert_called_once_with(path=file_path.parent)

    @patch("etl_assignment.steps.utils.ZipFile")
    def test_unzip_file_failure(self, MockZipFile: MagicMock) -> None:
        """Test unzip_file method when it raises an exception."""
        # Simulate an error in unzipping
        MockZipFile.side_effect = Exception("Unzip error")

        file_path = Path("/path/to/file.zip")
        with pytest.raises(Exception, match="Unzip error"):
            Utils.unzip_file(file_path)

    @patch("etl_assignment.steps.utils.requests.get")
    def test_download_file_success(self, mock_get: MagicMock) -> None:
        """Test download_file method when it succeeds."""
        # Set fake url
        url = "http://example.com/file.zip"
        # set request.get response
        mock_response = MagicMock()
        mock_get.return_value.__enter__.return_value = mock_response
        # set iter_content response
        # fmt: off
        mock_get.return_value.__enter__.return_value.iter_content.return_value = iter( [b"chunk1", b"chunk2"] ) # noqa
        # fmt: on
        # Call the method to test
        file_path = Utils.download_file(url)

        # Ensure the file is saved correctly
        expected_file_path = Path(TEMP_DIR) / "file.zip"
        assert file_path == expected_file_path

        # Ensure the correct request was made to the right URL
        mock_get.assert_called_once_with(url, stream=True)

        # Assert response used method raise_for_status
        mock_response.raise_for_status.assert_called_once()

        # Check that the file has been written to disk
        assert expected_file_path.exists()

        # Check if the content was written correctly
        with open(expected_file_path, "rb") as f:
            content = f.read()
            print(expected_file_path, content)
            assert (
                content == b"chunk1chunk2"
            )  # The expected content written to the file

    @patch("etl_assignment.steps.utils.requests.get")
    def test_download_file_failure(self, mock_get: MagicMock) -> None:
        """Test download_file method when it raises a RequestException."""
        # Simulate a failed request
        # fmt: off
        mock_get.side_effect = requests.exceptions.RequestException(
            "Download error"
        )
        # fmt: on

        url = "http://example.com/file.zip"
        with pytest.raises(
            requests.exceptions.RequestException, match="Download error"
        ):
            Utils.download_file(url)
