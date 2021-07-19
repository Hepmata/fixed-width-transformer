ResultMapper Documentation
==========================
Hello! In this section, we will cover the documentation about ResultMapper

Overview
========
ResultMapper has the following capabilities:

* ResultMapper: Maps Source data to Result format of your choosing
* Generator: Generates on the fly data and adds to Result
* Converter: Converts data to a data type of your choosing
* Validation: PostValidation after all the above operations are completed.

ResultMapper
************
ResultMapper is an aggregation class that collates all the above steps and executes them in the following order

ResultFormatter > Converter > Generator

Following the 3 steps, Validations are then triggered if any is requested.

ResultFormatter
***************
ResultFormatter 