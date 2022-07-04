from datetime import datetime, timedelta

from airflow import DAG
from utils.tags import Tag

from operators.gcp_container_operator import GKEPodOperator
from airflow.operators.dummy import DummyOperator

owner = "kignasiak@mozilla.com"

docs = """
### Data Monitoring - MVP

#### Description

This MVP is still under development and this is just first attempt at executing
data checks using Airflow

#### Owner

{owner}
""".format(owner=owner)


default_args = {
    "owner": owner,
    "email": [owner],
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 30),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

tags = [Tag.ImpactTier.tier_3, "MVP"]

great_expectations_checkpoints = (
    "internet_outages.global_outages",
    "telemetry_derived.active_users_aggregates",
)  # this could be dynamically retrieved using GE task listing expectations from store
# this will be okay to do once we upgrade Airflow to 2.3 (dynamic task mapping)

with DAG("data_monitoring_mvp", default_args=default_args, schedule_interval="0 0 * * *", doc_md=__doc__, tags=tags,) as dag:
    ge_tests_start = DummyOperator(
        task_id="ge_tests_start"
    )

    build_docs = GKEPodOperator(
        task_id="ge_build_docs",
        name="build_docs",
        image="ge:latest",
        cmds=["great_expectations", "docs", "build"],
    )

    for checkpoint in great_expectations_checkpoints:
        cmd = f"export _date_partition='{{{{ ds }}}}'; great_expectations --v3-api checkpoint run {checkpoint}"

        run_ge_tests = GKEPodOperator(
            task_id=f"ge_run_checkpoint.{checkpoint}",
            name="ge_internet_outages",
            image="ge:latest",
            cmds=[cmd],
        )

        ge_tests_start >> run_ge_tests >> build_docs
