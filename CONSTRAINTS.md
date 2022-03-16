# Constraints
Constraints is a high priority validation component in FileTransformer.
It's primary job is to validate high importance constraints, for example matching hashes or ensuring the file is valid based on database records.


## Difference between Constraints and Validators

### Priority
Constraints are higher priority than validators. Constraints are executed even before the Extraction phase in ETL whereas Validators are run only after Extraction phase.

### Data Access
Since Constraints runs before Extraction phase, this means it has no access to DataFrames and source file data. While it is possible to map the frames inside constraints, it should only be done if there is a hard requirement, or else using Validators would be better.


### Purpose
Constraints are meant to be general file checkers whereas Validators are field checkers.
Field checkers can access much more detailed data and should be used if that's the objective.

*Constraints Usage Example*: If you are trying to find out if the file name is registered in a SQL Database.

*Validator Usage Example*: If you are trying to find out a 'date' field in the file if its after today's date.


## Contributor Guide
As usual, the same rules that applies to Validators applies here as well.
1. Always detail the required arguments and data type.
2. Explain the flow and purpose of each class.
3. Code Quality is very important, please do not write unnecessary loops. Slow code = slow processing
4. Keep the class arguments generic. Do not introduce multiple abstract classes. Instead, use arguments.
5. **Before you write anything, check and see if there are similar Constraint first! You should contribute code, not recreate code.**


## Base Class Design
```python
class AbstractConstraint:
    def run(self, max_wait_time: float, wait_interval: float, arguments: dict):
        self.validate_arguments(arguments)

    def validate_arguments(self, arguments: dict): pass
```

In the Abstract class above, there are 2 defined methods.
1. `run`: runs the constraint, this is where your logic should exist
2. `validate_arguments`: validates the arguments.

### Usage
```python
class AbstractConstraint:
    def run(self, max_wait_time: float, wait_interval: float, arguments: dict):
        self.validate_arguments(arguments)

    def validate_arguments(self, arguments: dict): pass

class NewConstraint(AbstractConstraint):
    def run(self, max_wait_time: float, wait_interval: float, arguments: dict):
        super().run(arguments)

    def validate_arguments(self, arguments: dict): ...
```
Inherit the class, and use the super to validate arguments


# Available Constraints
The following section describes the available constraints

## HashConstraint
### Description
HashConstraint converts the source data file to hash based on provided algorithm
and compares it against another hash file to perform file integrity check.

### Required variables
- max_wait_time: float; Maximum time for method to wait, should not exceed 10 minutes.
- wait_interval: float: Interval time for retry, must be a lower value than max_wait_time
- arguments
  - bucket: str; S3 Bucket name holding the hash file
  - file_name: str; S3 Key or File Name of the hash file
  - algorithm: str; Algorithm of the Hash

### Exceptions
The following exceptions can occur if the method fails.
1. `InvalidConfigError` when the provided arguments fails the check
2. `FailedConstraintException` if the algorithm is wrong, hash function fails to hash, file can't be found or if the hash does not match.

## SQLConstraint
### Description
SQLConstraint issues SQL queries based on your input to a MySQL Database. SQLConstraint can perform field level and file level checks.
*The field level check only maps the first line of the file*

### Required variables
- max_wait_time: float; Maximum time for method to wait, should not exceed 10 minutes.
- wait_interval: float: Interval time for retry, must be a lower value than max_wait_time.
- arguments
  - secret_name: str; Secret name containing the username/password combo to your sql server
  - database_host: str; Database Host, should be a valid host such as `10.20.30.40`
  - database_name: str; Database Name that you are trying to connect to such as `mysql`
  - query: str; SQL Query to run on the database server. SQLConstraint works by a binary 1 or 0 system. Unless there's no row return, it will regard everything else as a success.

### Optional Arguments
On top of the compulsory arguments, the following optional arguments are also accepted.
- source_data_segment: str; Segment of data to use for source mapping, only supports header or footer.
- source_colnames: str; Column Names to map the segment data with an appropriate name.
- source_colspecs: str; colspecs to map the segment data. specs should be in brackets, comma seperated. Number of colspecs must be the same as colnames.
- database_port: str/number: If not using the default 3306 port, this argument should be provided to override the default.
- source_date_format: str; If referencing any dates from file, a format is required to correctly parse the date.

### Exceptions
The following exceptions can occur if the method fails.
1. `ConstraintMisconfigurationException` when the required arguments are not provided
2. `FailedConstraintException` if the method fails, including failure to execute query

### Examples
#### Field level check example
```yaml
...
  name: SqlConstraint
  max_wait_time: 100
  wait_interval: 6
  arguments:
    secret_name: /some/secret/name
    database_host: localhost:3306
    database_name: test_db
    source_data_segment: header
    source_colnames: createdDate, id, source_sender
    source_colspecs: (0,5),(5,10),(10,50)
    query: "SELECT * FROM tbl_file_history WHERE id = {id} AND file_name='{file_name}'"
```
#### Field level check with date format example
```yaml
...
  name: SqlConstraint
  max_wait_time: 100
  wait_interval: 6
  arguments:
    secret_name: /some/secret/name
    database_host: localhost:3306
    database_name: test_db
    database_port: 3907
    source_data_segment: header
    source_colnames: createdDate, id, source_sender
    source_colspecs: (0,5),(5,10),(10,50)
    source_date_format: %Y%m%d
    query: "SELECT * FROM tbl_file_history WHERE id = {id} AND file_name='{file_name}'"
```
#### No reference check example
```yaml
...
  name: SqlConstraint
  max_wait_time: 100
  wait_interval: 6
  arguments:
    secret_name: /some/secret/name
    database_host: localhost:3306
    database_name: test_db
    query: "SELECT * FROM tbl_file_history WHERE id = 1500"
```

#### No reference check example
```yaml
...
  name: SqlConstraint
  max_wait_time: 100
  wait_interval: 6
  arguments:
    secret_name: /some/secret/name
    database_host: localhost:3306
    database_name: test_db
    query: "SELECT * FROM tbl_file_history WHERE file_name = '{file_name}'"
```
