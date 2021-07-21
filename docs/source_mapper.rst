SourceMapper Documentation
==========================
In this section, we will cover the documentation about SourceMapper

Overview
========
SourceMapper has the following capabilities:

* SourceFormatter: Maps Source data to Result format of your choosing
* Validation: PostValidation after all the above operations are completed.
* Converter: Trims away all whitespaces in all data unless overridden in config.

SourceMapper
************
The execution of the above steps are as follows:
1. SourceFormatter to convert data from File to DataFrames
2. A NaN validation is then applied by default. To prevent this behaviour, provide an override in the config
3. Custom Validations are then executed if provided. Else this section will be skipped
4. Default Converter is then executed to trim away all whitespaces in DataFrames. To prevent this behaviour, provide and override in the config

ResultFormatter
***************
ResultFormatter