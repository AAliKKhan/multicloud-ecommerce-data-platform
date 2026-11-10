"""Spark Structured Streaming job and its Spark-free logic.

Only :mod:`ecomm.spark.job` imports PySpark (it runs in the Spark container).
The row classification, de-duplication step and batch sink are plain Python so
they can be unit-tested on any machine.
"""
