from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime

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
    
    @task
    def stop_data_generator():
        stop_process("data_generator.py")
    
    @task
    def stop_data_enrichment():
        stop_process("data_enrichment.py")

    (
        stop_data_generator()
        >>
        stop_data_enrichment()
    )

dag = pyspark_project_stop_processes()