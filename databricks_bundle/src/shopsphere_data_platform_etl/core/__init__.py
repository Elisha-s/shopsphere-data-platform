"""Business logic shared by every runtime.

Nothing in this package knows whether it is running inside a Lakeflow
pipeline on Databricks or inside a plain Spark session on a laptop: every
function takes DataFrames (or a SparkSession) and returns DataFrames. That is
what makes the logic unit-testable, and what lets the Azure pipeline and the
local pipeline run exactly the same transformations.
"""
