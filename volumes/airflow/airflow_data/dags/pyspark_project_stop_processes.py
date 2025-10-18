from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime
import time

log = LoggingMixin().log

@dag(
    dag_id="pyspark_project_stop_processes",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "pyspark", "project", "Rotem Reich", "Avi Yashar", "Kill", "Stop", "Processes"]
)

def pyspark_project_stop_processes():
    container_name = "dev_env"
    path = "/home/developer/projects/spark-course-python/spark_course_python/project/"

    def stop_process(file: str) -> str:
        command = f"docker exec {container_name} pkill -f {path}{file}"
    
        try:
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
            log.info("STDOUT: %s", result.stdout)
            log.info("STDERR: %s", result.stderr)
            log.info("\n\n\n", "Process '{file}' in '{container_name}' container was stopped", "\n\n\n")
            return f"Process '{file}' in '{container_name}' container was stopped"
        except subprocess.CalledProcessError as e:
            log.error("Error running '%s': returncode=%s", file, e.returncode)
            log.error("STDOUT:\n%s", e.stdout)
            log.error("STDERR:\n%s", e.stderr)
            raise
    
    def wait_task(seconds: int = 60):
        print(f">>>Waiting for {seconds} seconds...")
        time.sleep(seconds)
        print(f">>>{seconds} seconds are over!")
    
    @task
    def stop_data_generator():
        stop_process("data_generator.py")
    
    @task
    def wait_for_10_seconds():
        wait_task(10)
    
    @task
    def stop_data_enrichment():
        stop_process("data_enrichment.py")
    
    @task
    def wait_for_10_seconds2():
        wait_task(10)
    
    @task
    def stop_alerting_detection():
        stop_process("alerting_detection.py")

    (
        stop_alerting_detection()
        >> wait_for_10_seconds()
        >> stop_data_enrichment()
        >> wait_for_10_seconds2()
        >> stop_data_generator()
    )

dag = pyspark_project_stop_processes()