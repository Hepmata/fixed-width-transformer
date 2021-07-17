[![codecov](https://codecov.io/gh/SimpleConstructs/fixed-width-transformer/branch/master/graph/badge.svg?token=NP0R3Y98E1)](https://codecov.io/gh/SimpleConstructs/fixed-width-transformer)
[![coveralls](https://coveralls.io/repos/github/SimpleConstructs/fixed-width-transformer/badge.svg)](https://coveralls.io/github/SimpleConstructs/fixed-width-transformer)
[![license](https://img.shields.io/badge/license-Apache%202-blue.svg)](https://github.com/SimpleConstructs/fixed-width-transformer/blob/master/LICENSE)
# FixedWidth Transformer
Ever wanted to transform non-standard FixedWidth files from legacy systems to something modern?

Does your FixedWidth file have multiple segments(eg. header, body, footer, or worse)?

Do you want to save time and not spend hours of development processing these files?

You have arrived at the correct repository! 

FixedWidth Transformer is an open-source library that aims to solve tech debts of processing 
fixed width files to modern structures, such as json, xml. Not only that, it can also do generation of data(eg. uuid),
pre and post transform validations. It's even able to help you to push it to different destinations as well!
Best part of it? You don't have to write lines of code in order to use it.

Simply configure the .yaml config and run the relevant executors and call it a day.

# How it works
FixedWidth Transformer utilises a yaml based configuration. With the configuration, it uses 
pandas library to transform your data to DataFrames that from there allows you to do analytics operations
(eg. validations, concatenation, etc). 

The Library is highly extensible meaning that it can be extremely extensible in all sections of work,
including Executor, SourceMapper, SourceRetriever, ResultMapper, Result, Validation, and Generator.
This means that if something you need is not available, you can do incremental changes and just
add that section in, point to it in your configuration and be done with the requirement!

Neat huh?