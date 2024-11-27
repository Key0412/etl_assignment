import pandas as pd

from etl_assignment.general_config import get_config
from etl_assignment.logging_config import get_logger
from etl_assignment.pipeline import Pipeline

# isort: off
from etl_assignment.steps.extract import (
    ExtractURLfromXML,
    DownloadFile,
    ExtractXML,
    UnzipFile,
)
from etl_assignment.steps.load import SaveCSVLocally, UploadToBucket
from etl_assignment.steps.transform import (
    GenerateColumnsFromFullNm,
    TransformXML,
)

# isort: on

logger = get_logger()

TEMP_DIR, CHUNK_SIZE = get_config()

if __name__ == "__main__":
    # Define steps for the pipeline based on the task requirements
    steps = [
        # Requirement 1: download XML
        (
            ExtractXML,
            {
                "url": "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100",  # noqa
                "xpath": "//result//doc",
                "names": [
                    "checksum",
                    "download_link",
                    "publication_date",
                    "_root_",
                    "id",
                    "published_instrument_file_id",
                    "file_name",
                    "file_type",
                    "_version_",
                    "timestamp",
                ],
            },
        ),
        # Requirement 2: parse second link from XML and download XML from link
        (ExtractURLfromXML, {"file_type": "DLTINS", "n_doc": 1}),
        (DownloadFile, {}),
        # Requirement 3: extract XML from .zip
        (UnzipFile, {}),
        # Requirement 4: convert contents of XML to CSV with specific header
        (
            TransformXML,
            {
                "xpath": ".//default:FinInstrm/ModfdRcrd",
                "namespaces": {
                    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
                    "default": "urn:iso:std:iso:20022:tech:xsd:auth.036.001.02",  # noqa
                },
                "iterparse": {
                    "ModfdRcrd": [
                        "Id",
                        "FullNm",
                        "ClssfctnTp",
                        "CmmdtyDerivInd",
                        "NtnlCcy",
                        "Issr",
                    ]
                },
                "column_map": lambda col: (
                    "FinInstrmGnlAttrbts." + col if col != "Issr" else col
                ),
            },
        ),
        # Requirements 5 and 6: create new columns based on the count of 'a'
        (GenerateColumnsFromFullNm, {}),
        # Requirement 7: Store the CSV in an "AWS bucket"
        #               (using in memory filesystem instead)
        # Requirement 8: UploadToBucket is able to parse object storage
        #               adresses from Aws S3, Azure blob, and Google Cloud
        #               Storage by using the proper filesystem prefix,
        #               as in s3:, az:, and gs:.
        (SaveCSVLocally, {}),
        (
            UploadToBucket,
            {
                "bucket_address": "memory://s3-bucket",
                "key": "etl/output.csv",
                "storage_options": {},
            },
        ),  # defines 'bucket file' path from bucket_address + key
    ]

    logger.info("Starting ETL Pipeline from script")
    # Create and run the pipeline
    p = Pipeline(name="XML_ETL", steps=steps)  # type: ignore
    p.run_pipeline()
    # bucket_address + key
    bucket_file_path = "memory://s3-bucket/etl/output.csv"
    logger.info(f'Check file uploaded to "Bucket" {bucket_file_path}:')
    uploaded_csv = pd.read_csv(bucket_file_path)
    logger.info(f"\n{uploaded_csv.head()}")
