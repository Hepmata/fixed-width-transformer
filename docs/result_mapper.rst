ResultMapper Documentation
==========================
Hello! In this section, we will cover the documentation about ResultMapper

Overview
========
ResultMapper has the following capabilities:

* Generator: Generates data to be added to the Result data
* Converter: Converts data to a data type of your choosing.
* Validation: PostValidation after all the above operations are completed.
* ResultFormatter: Maps Source data to Result Format of your choosing

ResultMapper
************
The execution of the above steps are as follows:

1. **Generator** are executed first to add data to the data array
2. **Converter** are then executed to ensure data is in the type required. 
3. **Validation** are then executed to validate that the data is in good order
4. Finally, ResultFormatter generates the data in the format requested


ResultFormatter
***************
ResultFormatter