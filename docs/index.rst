.. FixedWidth Transformer documentation master file, created by
   sphinx-quickstart on Sun Jul 18 13:43:09 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to FixedWidth Transformer's documentation!
==================================================
FixedWidth Transformer is a flexible ETL(Extract, Transform, Load) framework
that aims to simplify the time required to process and push data.

Being a flexible framework, you can choose to do the full ETL or individual sections,
for example just extraction without transformation. or extraction and transformation without
loading to external sources.

.. note::
   This project is still under development.
.. toctree::
   :maxdepth: 2
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

Known Limitations
=================
There are a couple of known limitations with the design that you should be aware of.

* If the length of y our field is more than the 'spec' config given in 'source' segment, the system is unable to alert on this.
This should be expected since we are using pandas.read_fwf(). If you require this level of validation, you
might need to do it yourself in Executor.

* SourceMapping requires exactly 1 level of segment and cannot extend beyond that.
This should be expected as well since fixed width files are segmented at most by lines. This framework does not support
processing for data semgmentation like 'header.subheader' but only 'header'(single level).
=======
Overview
========


