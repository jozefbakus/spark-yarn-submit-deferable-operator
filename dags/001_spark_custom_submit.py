#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from datetime import datetime
from airflow.models import DAG
from spark_custom_submit_operator.spark_custom_submit_operator import SparkCustomSubmitOperator

with DAG(
    dag_id='001_spark_custom_submit',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    submit_job = SparkCustomSubmitOperator(
        appUniqueId="appUniqueId",
        appResource="/path/to/jar/spark-examples_2.11-2.4.8.jar",
        task_id="submit_job",
        java_class="org.apache.spark.examples.SparkPi",
        name="Test_Airflow_Spark_Submit",
        application_args=["400"],
        verbose=True,
        spark_binary="/path/to/spark-submit",
        resource_manager_url="http://domain:port/"
        env_vars={'HADOOP_CONF_DIR': '/path/to/hadoop'}
    )

submit_job