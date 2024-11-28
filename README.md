# ETL_assignment

An ETL pipeline codebase is implemented in this repository.
A base Step class is defined, which is used to implement arbitary ETL steps to be used in a pipeline.
A Pipeline class orchestrates the execution of the steps.

## Project Structure

The project structure is as follows:
- etl_assignment:
    - config: configuration files for general use and for logging
    - scripts: contains a script that instantiates a Pipeline for the assignment task. More below
    - steps: modules that define base Step class, steps that inherit from it, and utilities.
    These are organized as Extract.py, Transform.py, Load.py, Step.py, and Utils.py.
    - tests: unitests for the modules.
    - general_config.py: used to access general configuration file in config/.
    - logging_config.py: initializes logger from logging configuration file in config/.
    - pipeline.py: defines the Pipeline

## Task

The specific pipeline defined for this project contains the following step classes:

``ExtractXML -> ExtractURLfromXML -> DownloadFile -> UnzipFile -> TransformXML -> GenerateColumnsFromFullNm -> SaveCSVLocally -> UploadToBucket``

In order, these will collect a XML file, parse it to obtain a download URL, download the file, unzip it, parse it to obtain a CSV file with added columns, and finally store the file localy and in a mock bucket address.
The upload is object storage agnostic, leveraging fsspec and pandas capabilities.

The described pipeline is executed in the script etl_assignment/scripts/run_assignment_pipeline.py.

The generated table contains 425256 entries.
It's columns are:
```['FinInstrmGnlAttrbts.Id', 'FinInstrmGnlAttrbts.FullNm', 'FinInstrmGnlAttrbts.ClssfctnTp', 'FinInstrmGnlAttrbts.NtnlCcy', 'FinInstrmGnlAttrbts.CmmdtyDerivInd', 'Issr', 'a_count', 'contains_a']"```

The pipeline's resulting CSV first lines are:

```
  FinInstrmGnlAttrbts.Id                    FinInstrmGnlAttrbts.FullNm FinInstrmGnlAttrbts.ClssfctnTp FinInstrmGnlAttrbts.NtnlCcy  FinInstrmGnlAttrbts.CmmdtyDerivInd                  Issr  a_count contains_a
0                   WBAH                      EGB OE TL.Z./SARTORIUS V                         RWSNCA                         EUR                               False  PQOH26KWDF7CG10L6792        0         NO
1                   FRAB     Raiffeisen Centrobank AG TurboL O.End SAP                         RFSTCB                         EUR                               False  529900M2F7D5795H1A49        2        YES
2                   STUB     Turbo Long Open End Zertifikat auf SAP SE                         RFSTCB                         EUR                               False  529900M2F7D5795H1A49        2        YES
3                   WBAH                              RCB OE TL.Z./SAP                         RFSTCB                         EUR                               False  529900M2F7D5795H1A49        0         NO
4                   FRAB  Raiffeisen Centrobank AG TurboS O.End AIRBUS                         RFSTPB                         EUR                               False  529900M2F7D5795H1A49        2        YES
```


Tools used:
* Python 3
* pre-commit to apply linting (Ruff, black, flake8), formatting, and type checking (mypy).
* GithubActions to validate linting and automaticaly execute tests. Jobs for coverage computation and versioning are also present.
* poetry to manage dependencies
* logging to log executions
* Pandas to parse data
* fsspec to detect and create filesystem
* pytest for testing
