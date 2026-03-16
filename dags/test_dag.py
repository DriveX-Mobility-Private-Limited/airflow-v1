from airflow.decorators import dag, task
from datetime import timedelta
import pendulum


@dag(
    schedule=timedelta(hours=1),
    start_date=pendulum.yesterday(),
    catchup=False,
    tags=["test", "hourly"],
)
def test_hourly_dag():

    @task
    def print_hello():
        print("Hello from Airflow!")

    @task
    def print_success():
        print("DAG successful ran")

    print_hello() >> print_success()


test_hourly_dag()
