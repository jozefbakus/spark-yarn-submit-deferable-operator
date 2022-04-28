from airflow.models.baseoperator import BaseOperator
from threading import Timer
from typing import Any, Dict, List, Optional, Tuple, Iterator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import AirflowException
import requests
import subprocess
import asyncio
import os
import re

class SparkCustomSubmitOperator(BaseOperator):
    def __init__(
        self,
        *,
        appUniqueId: str,
        appResource: str,
        conf: Optional[Dict[str, Any]] = None,
        files: Optional[str] = None,
        py_files: Optional[str] = None,
        archives: Optional[str] = None,
        driver_class_path: Optional[str] = None,
        jars: Optional[str] = None,
        java_class: Optional[str] = None,
        packages: Optional[str] = None,
        exclude_packages: Optional[str] = None,
        repositories: Optional[str] = None,
        total_executor_cores: Optional[int] = None,
        executor_cores: Optional[int] = None,
        executor_memory: Optional[str] = None,
        driver_memory: Optional[str] = None,
        keytab: Optional[str] = None,
        principal: Optional[str] = None,
        proxy_user: Optional[str] = None,
        name: str,
        num_executors: Optional[int] = None,
        application_args: Optional[List[Any]] = None,
        env_vars: Optional[Dict[str, Any]] = None,
        verbose: bool = False,
        spark_binary: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._appUniqueId = appUniqueId
        self._appResource = appResource
        self._conf = conf or {}
        self._files = files
        self._py_files = py_files
        self._archives = archives
        self._driver_class_path = driver_class_path
        self._jars = jars
        self._java_class = java_class
        self._packages = packages
        self._exclude_packages = exclude_packages
        self._repositories = repositories
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._keytab = keytab
        self._principal = principal
        self._proxy_user = proxy_user
        self._name = name
        self._num_executors = num_executors
        self._application_args = application_args
        self._env_vars = env_vars
        self._verbose = verbose
        self._spark_binary = spark_binary

        self._submit_sp: Optional[Any] = None
        self._yarn_application_id: Optional[str] = None
        self._yarn_job_status: Optional[str] = None
        self._env: Optional[Dict[str, Any]] = None

    def execute(self, context):
        yarnResponse: YarnResponse = YarnApi.is_application_running_in_yarn_by_tag(self, self._appUniqueId)
        self._yarn_application_id = yarnResponse.yarn_application_id
        self._yarn_job_status = yarnResponse.yarn_job_status
        if yarnResponse.is_running:
            self.log.info(
                "Job has been running in YARN with application id: %s.",
                self._yarn_application_id
            )
            self._defer_()
        else:
            self.log.info("Job is not running in YARN. Going to submit a new instance")
            self._submit_()

    def on_kill(self):
        self.log.info(
            "Stopping any underlying process. Job may stay running in YARN under application id %s or application tag: %s",
            self._yarn_application_id,
            self._appUniqueId
        )
        if self._submit_sp:
            self._submit_sp.kill()

    def _submit_(self, **kwargs: Any) -> None:
        spark_submit_cmd = self._build_spark_submit_command()

        if self._env:
            env = os.environ.copy()
            env.update(self._env)
            kwargs["env"] = env

        self._submit_sp = subprocess.Popen(
            spark_submit_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=-1,
            universal_newlines=True,
            **kwargs,
        )

        timer = Timer(600 , self._submit_sp.kill)
        try:
            timer.start()
            self._process_spark_submit_log(iter(self._submit_sp.stdout))
        finally:
            timer.cancel()

        self._defer_()

    def _build_spark_submit_command(self) -> List[str]:
        spark_submit_command = [self._spark_binary]
        spark_submit_command += ["--master", 'yarn']

        for key in self._conf:
            spark_submit_command += ["--conf", f"{key}={str(self._conf[key])}"]


        tmpl = "spark.yarn.appMasterEnv.{}={}"
        self._env = self._env_vars
        for key in self._env_vars:
            spark_submit_command += ["--conf", tmpl.format(key, str(self._env_vars[key]))]

        spark_submit_command += ["--conf", f"spark.yarn.tags={str(self._appUniqueId)}"]

        if self._files:
            spark_submit_command += ["--files", self._files]
        if self._py_files:
            spark_submit_command += ["--py-files", self._py_files]
        if self._archives:
            spark_submit_command += ["--archives", self._archives]
        if self._driver_class_path:
            spark_submit_command += ["--driver-class-path", self._driver_class_path]
        if self._jars:
            spark_submit_command += ["--jars", self._jars]
        if self._packages:
            spark_submit_command += ["--packages", self._packages]
        if self._exclude_packages:
            spark_submit_command += ["--exclude-packages", self._exclude_packages]
        if self._repositories:
            spark_submit_command += ["--repositories", self._repositories]
        if self._num_executors:
            spark_submit_command += ["--num-executors", str(self._num_executors)]
        if self._total_executor_cores:
            spark_submit_command += ["--total-executor-cores", str(self._total_executor_cores)]
        if self._executor_cores:
            spark_submit_command += ["--executor-cores", str(self._executor_cores)]
        if self._executor_memory:
            spark_submit_command += ["--executor-memory", self._executor_memory]
        if self._driver_memory:
            spark_submit_command += ["--driver-memory", self._driver_memory]
        if self._keytab:
            spark_submit_command += ["--keytab", self._keytab]
        if self._principal:
            spark_submit_command += ["--principal", self._principal]
        if self._proxy_user:
            spark_submit_command += ["--proxy-user", self._proxy_user]
        if self._name:
            spark_submit_command += ["--name", self._name]
        if self._java_class:
            spark_submit_command += ["--class", self._java_class]
        if self._verbose:
            spark_submit_command += ["--verbose"]

        spark_submit_command += ["--queue", 'default']
        spark_submit_command += ["--deploy-mode", 'cluster']

        spark_submit_command += [self._appResource]

        if self._application_args:
            spark_submit_command += self._application_args

        self.log.info("Spark-Submit cmd: %s", spark_submit_command)
        return spark_submit_command

    def _process_spark_submit_log(self, itr: Iterator[Any]) -> None:
        for line in itr:
            line = line.strip()
            self.log.info(line)

            if not self._yarn_application_id:
                applicationIdMatch = re.search('(application[0-9_]+)', line)
                if applicationIdMatch:
                    self._yarn_application_id = applicationIdMatch.groups()[0]
                    self.log.info("Identified spark driver id: %s", self._yarn_application_id)

            if not self._yarn_job_status:
                stateMatch = re.search('(?:state:\s)([A-Z]+)', line)
                if stateMatch:
                    self._submit_sp.kill()
                    self._yarn_job_status =  stateMatch.groups()[0]
                    self.log.info("Yarn job status: %s", self._yarn_job_status)

    def _afterDefer(self, context, event=None):
        if event:
            if event == "SUCCEEDED":
                self.log.info("Job has finished with SUCCEEDED Status")
            else:
                self.log.info("Job has failed with %s Status", event)
                raise AirflowException("Job has finished with ", event, " Status")
        else:
            self.log.info("Job has finished with UNKNOWN Status")
            raise AirflowException("Job has finished with UNKNOWN Status")

    def _defer_(self) -> None:
        self.log.info(
            "Going to deffer and keep track of job status by YARN application id: %s or application tag: %s ",
            self._yarn_application_id,
            self._appUniqueId
        )
        self.defer(
            trigger=SparkCustomSubmitTrigger(yarn_application_id=self._yarn_application_id, appUniqueId=self._appUniqueId),
            method_name="_afterDefer"
        )

class SparkCustomSubmitTrigger(BaseTrigger):
    def __init__(self, appUniqueId: str, yarn_application_id: Optional[str] = None):
        super().__init__()
        self._yarn_application_id = yarn_application_id
        self._appUniqueId = appUniqueId
        self._yarn_job_status = None

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        return (
            "spark_custom_submit_operator.spark_custom_submit_operator.SparkCustomSubmitTrigger",
            {"yarn_application_id": self._yarn_application_id, "appUniqueId": self._appUniqueId}
        )

    async def run(self):
        while True:
            await asyncio.sleep(5)
            if not self._yarn_application_id:
                self.log.info("Getting job status from YARN by application tag: %s", self._appUniqueId)
                yarnResponse: YarnResponse = YarnApi.is_application_running_in_yarn_by_tag(self, self._appUniqueId)
                self._yarn_application_id = yarnResponse.yarn_application_id
                self._yarn_job_status = yarnResponse.yarn_job_status
            else:
                self.log.info("Getting job status from YARN by yarn application id: %s", self._yarn_application_id)
                yarnResponse: YarnResponse = YarnApi.is_application_running_in_yarn_by_yarn_application_id(self, self._yarn_application_id)
                self._yarn_job_status = yarnResponse.yarn_job_status

            if self._yarn_job_status == "UNDEFINED":
                self.log.info(
                    "Job is still running in YARN, application id: %s or application tag: %s",
                    self._yarn_application_id,
                    self._appUniqueId
                )
            else:
                self.log.info(
                    "Job has finished in YARN, application id: %s or application tag: %s with status: %s",
                    self._yarn_application_id,
                    self._appUniqueId,
                    self._yarn_job_status
                )
                yield TriggerEvent(self._yarn_job_status)

    def cleanup(self) -> None:
        self.log.info(
            "Spark custom submit trigger is finishing for YARN application id: %s or application tag: %s with status: %s",
            self._yarn_application_id,
            self._appUniqueId,
            self._yarn_job_status
        )



class YarnResponse:
    def __init__(
        self,
        is_running: bool,
        yarn_application_id: Optional[str],
        yarn_job_status: Optional[str]
    ) -> None:
        self.is_running: bool = is_running
        self.yarn_application_id: Optional[str] = yarn_application_id
        self.yarn_job_status: Optional[str] = yarn_job_status

class YarnApi():
    # YARN FINAL STATUS: UNDEFINED, SUCCEEDED, FAILED, KILLED

    @staticmethod
    def is_application_running_in_yarn_by_tag(self, appUniqueId: str) -> YarnResponse:
        response = requests.get(f"http://localhost:8088/ws/v1/cluster/apps?applicationTags={appUniqueId}&finalStatus=UNDEFINED")
        if response.status_code == 200:
            if 'app' in response.json()['apps']:
                try:
                    self._yarn_application_id = response.json()['apps']['app'][0]['id']
                    self._yarn_job_status = response.json()['apps']['app'][0]['finalStatus']
                except:
                    raise AirflowException("Could not extract application id or application status from YARN response. Giving up.")
                return YarnResponse(is_running = True, yarn_application_id = self._yarn_application_id, yarn_job_status = self._yarn_job_status)
            else:
                return YarnResponse(is_running = False, yarn_application_id = self._yarn_application_id, yarn_job_status = self._yarn_job_status)
        else:
            raise AirflowException("Could not get status from YARN. Giving up.")

    @staticmethod
    def is_application_running_in_yarn_by_yarn_application_id(self, yarn_application_id: str) -> YarnResponse:
        response = requests.get(f"http://localhost:8088/ws/v1/cluster/apps/{yarn_application_id}")
        if response.status_code == 200:
            try:
                self._yarn_job_status = response.json()['app']['finalStatus']
            except:
                raise AirflowException("Could not extract application id from YARN response. Giving up.")
            return YarnResponse(is_running = True, yarn_application_id = self._yarn_application_id, yarn_job_status = self._yarn_job_status)
        else:
            raise AirflowException("Could not get status from YARN. Giving up.")