# How to run example ? 
- Place 001_spark_custom_submit.py file into dags directory in your local airflow
  - Update appResource, spark_binary and env_vars parameters based on your env. 
- Place spark_custom_submit_operator dir with file spark_custom_submit_operator.py into plugin directory in your local airflow

# What does plugin do ?
spark_custom_submit_operator is a wrapper around the spark-submit binary to kick off a spark job in YARN.
Once job is in yarn, operator defers and releases airflow worker space and leaves status checking of a job in Yarn on airflow triggerer.
