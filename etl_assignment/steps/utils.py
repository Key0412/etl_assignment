from pathlib import Path
from zipfile import ZipFile

import requests  # type: ignore

from etl_assignment.general_config import get_config
from etl_assignment.logging_config import get_logger

logger = get_logger()

TEMP_DIR, CHUNK_SIZE = get_config()


class Utils:
    """Utility class for common file operations."""

    @staticmethod
    def unzip_file(file_path: Path) -> None:
        """Unzips a file to its parent directory.

        Args:
            file_path (Path): The path to the zip file to be extracted.

        Raises:
            Exception: If an error occurs during unzipping.
        """
        try:
            with ZipFile(file_path, "r") as file:
                logger.info(f"Unzipping file: {file_path}")
                file.extractall(path=file_path.parent)
            logger.info(f"File unzipped to: {file_path.parent}")
        except Exception as e:
            logger.error(f"Error unzipping file {file_path}")
            raise e

    @staticmethod
    def download_file(url: str) -> Path:
        """Downloads a file from URL and saves it in the temporary directory.

        Args:
            url (str): The URL of the file to download.

        Returns:
            Path: The path to the downloaded file.

        Raises:
            requests.exceptions.RequestException: Error in download process.
        """
        file_path = Path(TEMP_DIR).joinpath(url.split("/")[-1])
        file_path.parent.mkdir(exist_ok=True)
        logger.info(f"Downloading file from: {url}")
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(file_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
            logger.info(f"File downloaded successfully: {file_path}")
        except requests.exceptions.RequestException as e:
            logger.error("Error during file download")
            raise e
        return file_path
