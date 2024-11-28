# isort: off
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from etl_assignment.logging_config import get_logger, setup_logging
from etl_assignment.steps.extract import (
    DownloadFile,
    ExtractURLfromXML,
    ExtractXML,
    UnzipFile,
)

setup_logging()
logger = get_logger()
logger.debug("Testing Extract")


class TestExtractXML:
    """Tests for ExtractXML class."""

    @patch("etl_assignment.steps.extract.read_xml")
    def test_run_step_success(self, mock_read_xml: MagicMock) -> None:
        """Test the run_step method when it succeeds."""
        # Prepare mock response
        mock_df = pd.DataFrame(
            {"download_link": ["http://example.com"], "file_type": ["xml"]}
        )
        mock_read_xml.return_value = mock_df

        # Initialize the ExtractXML instance
        extract_xml = ExtractXML(
            url="http://example.com/file.xml",
            xpath="//item",
            names=["file_type", "download_link"],
        )

        # Run the method
        extract_xml.run_step()

        # Verify if the DataFrame was parsed correctly
        mock_read_xml.assert_called_once_with(
            "http://example.com/file.xml",
            xpath="//item",
            names=["file_type", "download_link"],
        )
        assert "df" in extract_xml.step_result
        assert isinstance(extract_xml.step_result["df"], pd.DataFrame)

    @patch("etl_assignment.steps.extract.read_xml")
    def test_run_step_failure(self, mock_read_xml: MagicMock) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate an error in reading XML
        mock_read_xml.side_effect = Exception("Parsing error")

        extract_xml = ExtractXML(
            url="http://example.com/file.xml",
            xpath="//item",
            names=["file_type", "download_link"],
        )

        with pytest.raises(Exception, match="Parsing error"):
            extract_xml.run_step()


class TestExtractURLfromXML:
    """Tests for ExtractURLfromXML class."""

    def test_run_step_success(self) -> None:
        """Test the run_step method when it succeeds."""
        # Mock the DataFrame
        df = pd.DataFrame(
            {
                "file_type": ["pdf", "txt"],
                "download_link": [
                    "http://example.com/file1.pdf",
                    "http://example.com/file2.txt",
                ],
            }
        )

        # Initialize ExtractURLfromXML with the mock DataFrame
        extract_url = ExtractURLfromXML(df=df, file_type="pdf", n_doc=0)

        # Run the method
        extract_url.run_step()

        # Check the step result
        assert "download_link" in extract_url.step_result
        # fmt: off
        assert (
            extract_url.step_result["download_link"] == "http://example.com/file1.pdf" # noqa
        )
        # fmt: on

    def test_run_step_failure(self) -> None:
        """Test the run_step method when no document is found."""
        df = pd.DataFrame(
            {
                "file_type": ["txt"],
                "download_link": ["http://example.com/file2.txt"],
            }
        )

        extract_url = ExtractURLfromXML(df=df, file_type="pdf", n_doc=0)

        with pytest.raises(Exception):
            extract_url.run_step()


class TestDownloadFile:
    """Tests for DownloadFile class."""

    @patch("etl_assignment.steps.extract.Utils.download_file")
    def test_run_step_success(self, mock_download_file: MagicMock) -> None:
        """Test the run_step method when it succeeds."""
        # Mock the download_file method
        mock_download_file.return_value = Path("/path/to/downloaded/file.zip")
        # fmt: off
        download_file = DownloadFile(download_link="http://example.com/file.zip")  # noqa
        # fmt: on
        # Run the method
        download_file.run_step()

        # Verify the result
        assert "file_path" in download_file.step_result
        assert download_file.step_result["file_path"] == Path(
            "/path/to/downloaded/file.zip"
        )

    @patch("etl_assignment.steps.extract.Utils.download_file")
    def test_run_step_failure(self, mock_download_file: MagicMock) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate an error in downloading
        mock_download_file.side_effect = Exception("Download error")
        # fmt: off
        download_file = DownloadFile(download_link="http://example.com/file.zip")  # noqa
        # fmt: on
        with pytest.raises(Exception, match="Download error"):
            download_file.run_step()


class TestUnzipFile:
    """Tests for UnzipFile class."""

    @patch("etl_assignment.steps.extract.Utils.unzip_file")
    def test_run_step_success(self, mock_unzip_file: MagicMock) -> None:
        """Test the run_step method when it succeeds."""
        # Mock the unzip_file method
        mock_unzip_file.return_value = None

        file_path = Path("/path/to/file.zip")
        unzip_file = UnzipFile(file_path=file_path)

        # Run the method
        unzip_file.run_step()

        # Check the result
        assert "file_path" in unzip_file.step_result
        # fmt: off
        assert unzip_file.step_result["file_path"] == file_path.with_suffix(".xml")  # noqa
        # fmt: on

    @patch("etl_assignment.steps.extract.Utils.unzip_file")
    def test_run_step_failure(self, mock_unzip_file: MagicMock) -> None:
        """Test the run_step method when it raises an exception."""
        # Simulate an error in unzipping
        mock_unzip_file.side_effect = Exception("Unzip error")

        file_path = Path("/path/to/file.zip")
        unzip_file = UnzipFile(file_path=file_path)

        with pytest.raises(Exception, match="Unzip error"):
            unzip_file.run_step()
