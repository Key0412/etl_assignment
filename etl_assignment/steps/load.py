from pathlib import Path

from pandas import DataFrame, read_csv

from etl_assignment.general_config import get_config
from etl_assignment.logging_config import get_logger
from etl_assignment.steps.step import Step

logger = get_logger()

TEMP_DIR, CHUNK_SIZE = get_config()


class SaveCSVLocally(Step):
    def __init__(
        self, data: DataFrame, file_path: str = f"{TEMP_DIR}/output.csv"
    ) -> None:
        """Saves a DataFrame as a CSV file.

        Args:
            data (DataFrame): The DataFrame to be saved as a CSV.
            file_path (str, optional): Path where the CSV file will be saved.
                Defaults to f'{TEMP_DIR}/output.csv'.
        """
        super().__init__()
        self.data = data
        self.file_path = file_path
        self.class_name = self.__class__.__name__

    def run_step(self) -> None:
        """Saves the DataFrame to a local CSV file at the specified file path.

        Raises:
            Exception: Issue occurs while saving the DataFrame as a CSV file.
        """
        try:
            logger.info(f"{self.class_name}: Saving DataFrame as CSV")
            fp = Path(self.file_path)
            fp.parent.mkdir(exist_ok=True)
            self.data.to_csv(fp, index=False)
            self.step_result = {"file_path": self.file_path}
            logger.info(f"{self.class_name}: CSV saved {self.file_path}")
        except Exception as e:
            logger.error(f"{self.class_name}: Error saving CSV")
            raise e


class UploadToBucket(Step):
    def __init__(
        self,
        file_path: str,
        bucket_address: str,
        key: str,
        storage_options: dict[str, str],
    ) -> None:
        """Uploads a file to a cloud storage bucket.

        The class can upload files to Amazon S3, Google Cloud Storage, and
        Azure Blob Storage.
        It automatically detects the target service based on the prefix in the
        bucket address (e.g., 's3', 'az', 'gs').
        Each service requires specific authentication credentials, which must
        be provided through the `storage_options` parameter.

        Args:
            file_path (str): The local path to the CSV file to be uploaded.
            bucket_address (str): The address of the cloud storage bucket.
            key (str): Key for the uploaded file in the bucket.
            storage_options (dict[str, str]): Authentication credentials
                and other configuration options specific to the cloud service.

        Examples:
            For AWS S3 Storage:
                file_path_aws = 's3://my-bucket/my-folder/my-file.csv'

            For Azure Blob Storage:
                file_path_azure = 'az://my-bucket/my-folder/my-file.csv'

            For Google Cloud Storage:
                file_path_gcs = 'gs://my-bucket/my-folder/my-file.csv'
        """
        super().__init__()
        self.file_path = file_path
        self.key = key
        self.bucket_address = bucket_address
        self.storage_options = storage_options
        self.class_name = self.__class__.__name__

    def run_step(self) -> None:
        """Uploads the CSV file to the specified cloud storage bucket.

        Raises:
            Exception: If any issue occurs during the file upload process.
        """
        try:
            bucket_path = f"{self.bucket_address}/{self.key}"
            logger.info(f"{self.class_name}: {self.file_path}->{bucket_path}")
            df = read_csv(Path(self.file_path))
            df.to_csv(
                bucket_path,
                index=False,
                chunksize=CHUNK_SIZE,
                storage_options=self.storage_options,
            )
            self.step_result = {"bucket_path": bucket_path}
            logger.info(f"{self.class_name}: CSV uploaded")
        except Exception as e:
            logger.error(f"{self.class_name}: Error uplading CSV to bucket")
            raise e
