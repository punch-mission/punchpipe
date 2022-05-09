Welcome to punchpipes's documentation!
==========================================


The ``punchpipe`` provides the basic tools to process data from the `PUNCH mission`_. It is managed using `Prefect`_,
a data pipeline orchestration software. However, you do *not* need to understand `Prefect`_ in order to use this
pipeline.

Pipeline layout
===============
The pipeline is organized into several segments that connect different levels of products. The levels are as follows:

* Raw: CCSDS packets straight from the Mission Operations Center
* Level 0: Decoded images with no processing applied
* Level 1: Calibrated, cosmic-ray despiked, destreaked, stray-light subtracted and aligned images
* Level 2: Polarization-resolved, quality-marked trefoils
* Level 3: Final products with the F-corona, starfield, and background subtracted

Thus, the segments connect these levels as follows:

* Raw to Level 0
* Level 0 to Level 1
* Level 1 to Level 2
* Level 2 to Level 3

When you see a segment referred to with just one level, it is referring to the ending level. For example, the
Level 1 segment means the Level 0 to Level 1 segment. This shortened convention is used to organize the code into
subpackages.

Running a segment
=================
As an end-user, you can run segments of the pipeline by importing them from their ``flow`` module in that segment's
subpackage. This is detailed more explicitly in the `Running Pipeline tutorial`_.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _PUNCH mission: https://punch.space.swri.edu/

.. _Prefect: https://prefect.io

.. _Running Pipeline tutorial: running.html

.. toctree::
   :maxdepth: 2
   :caption: Contents:

    Getting started <getting-started>
    Running the Pipeline <running>
    API <code_ref/index>
