from pathlib import Path

from pandas import DataFrame, read_xml

from etl_assignment.logging_config import get_logger
from etl_assignment.steps.step import Step
from etl_assignment.steps.utils import Utils

logger = get_logger()


class ExtractXML(Step):
    def __init__(self, url: str, xpath: str, names: list[str]) -> None:
        """Extracts XML content from URL and parses it into a DataFrame.

        Args:
            url (str): The URL of the XML file.
            xpath (str): The XPath expression to parse the XML.
            names (list[str]): The column names for the DataFrame.
        """
        super().__init__()
        self.url = url
        self.xpath = xpath
        self.names = names

    def run_step(self) -> None:
        """Parses XML from URL using the XPath and converts it to a DataFrame.

        Raises:
            Exception: If there is an issue during XML parsing.
        """
        try:
            logger.info(
                f"{self.__class__.__name__}: Extracting XML from: {self.url}")
            df = read_xml(self.url, xpath=self.xpath, names=self.names)
            self.step_result = {"xml_df": df}
            logger.info(f"{self.__class__.__name__}: XML parsed to DataFrame")
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Unable to parse XML")
            raise e


class ExtractURLfromXML(Step):
    def __init__(self, xml_df: DataFrame, file_type: str, select_document: int) -> None:
        """Extracts download link from XML DataFrame.
        Filters XML using file type element and document index.

        Args:
            xml_df (DataFrame): The DataFrame containing XML data.
            file_type (str): The file type to filter by in the XML data.
            select_document (int): 0-based index of the document to select.
        """
        super().__init__()
        self.xml_df = xml_df
        self.file_type = file_type
        self.select_document = select_document

    def run_step(self) -> None:
        """Extracts download URL for specified file type and document index.

        Raises:
            IndexError: If no document is found.
        """
        try:
            logger.info(
                f"""{self.__class__.__name__}: Extracting
                URL #{self.select_document} file type {self.file_type}"""
            )
            url = (
                self.xml_df["download_link"]
                .loc[self.xml_df["file_type"] == self.file_type]
                .values[self.select_document]
            )
            self.step_result = {"download_link": url}
            logger.info(
                f"{self.__class__.__name__}: URL parsed from XML: {url}")
        except IndexError as e:
            logger.error(f"{self.__class__.__name__}: No document found")
            raise e


class DownloadFile(Step):
    def __init__(self, download_link: str, utils: Utils = Utils()) -> None:
        """Downloads a file from a given URL.

        Args:
            download_link (str): The URL of the file to download.
            utils (Utils, optional): Utility class handles file operations.
                Defaults to Utils().
        """
        super().__init__()
        self.download_link = download_link
        self.utils = utils

    def run_step(self) -> None:
        """Downloads the file from the provided download link.

        Raises:
            Exception: If an error occurs during the download process.
        """
        try:
            logger.info(
                f"""{self.__class__.__name__}:
                Downloading from: {self.download_link}"""
            )
            file_path = self.utils.download_file(url=self.download_link)
            self.step_result = {"file_path": file_path}
            logger.info(
                f"""{self.__class__.__name__}:
                    File downloaded successfully: {file_path}"""
            )
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Error downloading file")
            raise e


class UnzipFile(Step):
    def __init__(self, file_path: Path, utils: Utils = Utils()) -> None:
        """Unzips a downloaded file.

        Args:
            file_path (Path): The path to the zip file.
            utils (Utils, optional): Utility class handles file operations.
                Defaults to Utils().
        """
        super().__init__()
        self.file_path = file_path
        self.utils = utils

    def run_step(self) -> None:
        """Unzips the file at the specified path.

        Raises:
            Exception: If an error occurs during unzipping.
        """
        try:
            logger.info(
                f"{self.__class__.__name__}: Unzipping: {self.file_path}")
            self.utils.unzip_file(file_path=self.file_path)
            unzipped_path = self.file_path.with_suffix(".xml")
            self.step_result = {"file_path": unzipped_path}
            logger.info(
                f"""{self.__class__.__name__}:
                    File unzipped successfully: {unzipped_path}"""
            )
        except Exception as e:
            logger.error(f"{self.__class__.__name__}: Error unzipping file")
            raise e
