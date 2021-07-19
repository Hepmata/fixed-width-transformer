.. FixedWidth Transformer documentation master file, created by
   sphinx-quickstart on Sun Jul 18 13:43:09 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to FixedWidth Transformer's documentation!
==================================================

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