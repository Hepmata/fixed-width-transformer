# Description

FileTransformer is a ingest ETL solution. Files are processed based on your provided configuration and can provide the following functions:
- Extraction of files based on regex pattern
- Transformation of file data to your requested format
- Loading of transformed data to multiple available destinations.

##  How It works
FileTransformer executes the following tasks in order:
1. Config Retrieval
2. Config Loading
3. Source File Retrieval
4. Constraints
5. Source File Extraction
6. Source Validation
7. Source to Result Transformation
8. Result Validation
9. Publish Result

### Config Retrieval
Configuration Retrieval can be either local or remote config.

Local config is packaged along with the lambda codebase, does not support hotloading.
Remote config is stored in a S3 Bucket, supports hotloading.

### Config Loading
After retrieval of config, the config is loaded through a config parser so that the
main program can easily access the config information. Config Loading is singleton based
so the config service does not do multiple re-parsing per config access.

### Source File Retrieval
Source File information comes from 2 sources:
1. AWS Lambda Event: Contains the S3 PUT event information that triggered FileTransformer.
2. Config: Contains the regex that correlates the file name to config segment.

Currently, only 2 types of files are supported:
1. CSV
2. Fixed Width

With both of the required information now available, source file can now be retrieved from S3 via API for the next step.


### Constraints
Refer to [Constraints](CONSTRAINTS.md) for more information.

### Source File Extraction
In ETL phases, Extraction refers to the parsing of the file.

With the S3 file retrieved, the file is then parsed using the provided source file config and pandas to form processable DataFrames for later stages.

### Source Validation
FileTransformer supports validation with the help of custom extension validators. These validators are used to ensure that the data adheres
to the expected data format before the data enters the core microservices.
The current supported Validators are as follows:
1. SqlValidator
2. NricValidator
3. RegexValidator
4. NaNValidator
5. DuplicateGroupedValidator
6. TotalReferenceValidator
7. RefValidator
8. DateValidator
9. MinimumAmountValidator

For more information on the validators, please visit [Validators](VALIDATORS.md) for detailed information

If any segment of the file fails the validators, a error will be logged and the processing will stop.

### Source to Result Transformation
Once the validation is done, we can perform the next step in ETL, which is Transformation.
Transformation refers to converting the data in Extraction phase, to a format of your specification.

The transformed data is formatted and stored back into a new DataFrame.

### Result Validation
With the transformed DataFrame, a secondary validation can be done to ensure that the data is still in our expected format.

Note: You should not repeat the validations in Source and Results. Results validation is more for validating the result format.
(eg. column contains only float type data and not int type)

If any segment of the file fails the validators, a error will be logged and the processing will stop.

### Publish Result
After result validation, our data is finally available for the final ETL phase, Loading.
Loading refers to transferring the data to a new destination.

Result DataFrame is now converted into json messages and is sent to a destination based on the provided configuration.
The following destinations are supported:
1. Kafka (SASL Auth only)
2. S3

With that, FileTransformer has completed the ETL process and will emit a processing result message to AWS SQS.

##  Configuration
Refer to [Config](CONFIG.md) for more information
