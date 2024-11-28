from pathlib import Path
from typing import Callable, Union

from pandas import DataFrame, Series, read_xml

from etl_assignment.logging_config import get_logger
from etl_assignment.steps.step import Step

logger = get_logger()


class TransformXML(Step):
    def __init__(
        self,
        file_path: Path,
        xpath: str,
        namespaces: dict[str, str],
        iterparse: dict[str, list[str]],
        column_map: Union[Callable[[str], str], None] = None,
    ) -> None:
        """Transforms XML file into a DataFrame by parsing specific elements.

        Args:
            file_path (Path): The path to the XML file.
            xpath (str): XPath expression to extract elements from the XML.
            namespaces (dict): The namespaces used in the XML document.
            iterparse (dict[str, list]): Instructions to parse XML in chunks.
            column_map (Union[Callable[[str], str], None], optional):
                A function to map column names. Defaults to None.
        """
        super().__init__()
        self.file_path = file_path
        self.xpath = xpath
        self.namespaces = namespaces
        self.iterparse = iterparse
        self.column_map = column_map
        self.class_name = self.__class__.__name__

    def run_step(self) -> None:
        """Parses XML file into DataFrame, optionally remapping column names.

        Raises:
            Exception: If an error occurs during the XML parsing.
        """
        try:
            logger.info(f"{self.class_name}: XML to DF: {self.file_path}")
            df = read_xml(
                self.file_path,
                xpath=self.xpath,
                namespaces=self.namespaces,
                iterparse=self.iterparse,
            )
            try:
                df.columns = df.columns.map(self.column_map)
            except TypeError:
                pass
            self.step_result = {"data": df}
            logger.info(f"{self.class_name}: XML processed")
        except Exception as e:
            logger.error(f"{self.class_name}: Error parsing XML to DF")
            raise e


def _generate_a_columns(x: Series) -> tuple[int, str]:
    """Generates count of 'a' characters in the string and a
    flag indicating if 'a' is present.

    Args:
        x (Series): A pandas Series object containing a string column.

    Returns:
        tuple[int, str]: First element is the count of 'a' characters,
            and the second element is 'YES' if 'a' is present, otherwise 'NO'.
    """
    a_count = x.values[0].count("a")
    contains_a = "YES" if a_count else "NO"
    return a_count, contains_a


class GenerateColumnsFromFullNm(Step):
    def __init__(self, data: DataFrame) -> None:
        """Processes the 'FinInstrmGnlAttrbts.FullNm' column.

        Args:
            data (DataFrame): A pandas DataFrame that contains
                the column 'FinInstrmGnlAttrbts.FullNm'.
        """
        super().__init__()
        self.data = data
        self.class_name = self.__class__.__name__

    def run_step(self) -> None:
        """Generates columns: 'a_count' and 'contains_a'.

        'a_count' contains the number of 'a' characters
            in each entry of the 'FinInstrmGnlAttrbts.FullNm' column,
        'contains_a' is a flag for whether or not the string contains 'a'.

        Raises:
            Exception: Issue during the generation of the new columns.
        """
        try:
            logger.info(f"{self.class_name}: Generating 'a' count columns")
            self.data[["a_count", "contains_a"]] = self.data[
                ["FinInstrmGnlAttrbts.FullNm"]
            ].apply(_generate_a_columns, result_type="expand", axis=1)
            self.step_result = {"data": self.data}
            logger.info(f"{self.class_name}: Columns created successfully")
        except Exception as e:
            logger.error(f"{self.class_name}: Error creating columns")
            raise e
