from airflow.decorators import dag, task
#from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime
import time
import os
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError


log = LoggingMixin().log

@dag(
    dag_id = "pyspark_project_rotem_dims",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "pyspark", "project", "Rotem Reich", "Avi Yashar"]
)

def pyspark_project_pipeline():
    
    container_name = "dev_env"
    python_path = "/usr/bin/python3"
    path = "/home/developer/projects/spark-course-python/spark_course_python/project/"
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_token = os.getenv("AWS_SESSION_TOKEN")
    course_broker = "course-kafka:9092"
    
    def run_command(file, detached=False, minio=False):
        base_cmd = f"docker exec"
        
        if detached:
           base_cmd += " -d"
        
        if minio:
            if not aws_access_key or not aws_secret_key:
                raise ValueError("Missing AWS credentials in environment")
            else:
                base_cmd += f" -e AWS_ACCESS_KEY_ID={aws_access_key} -e AWS_SECRET_ACCESS_KEY={aws_secret_key}"
            
            if aws_token:
                base_cmd += f" -e AWS_SESSION_TOKEN={aws_token}"
        
        command = f"{base_cmd} {container_name} {python_path} {path}{file}"
              
        try:
            result = subprocess.run(command ,shell=True, check=True, capture_output=True, text=True)
            log.info("STDOUT: %s", result.stdout)
            log.info("STDERR: %s", result.stderr)
            return f"'{file}' Script executed successfully"
        except subprocess.CalledProcessError as e:
            log.error("Error running '%s': returncode=%s", file, e.returncode)
            log.error("STDOUT:\n%s", e.stdout)
            log.error("STDERR:\n%s", e.stderr)
            raise
    
    @task
    def wait_task(seconds=60):
        print(f">>>Waiting for {seconds} seconds...")
        time.sleep(seconds)
        print(f">>>{seconds} seconds are over!")
    
    @task
    def wait_for_message(topic: str):
        max_timeout_seconds = 120
        poll_interval = 10
        start_time = time.time()
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers = course_broker,
            auto_offset_reset = "earliest",
            enable_auto_commit = False,
            group_id = "airflow-wait-group",  # use a fixed group to always get earliest
            consumer_timeout_ms = 5000,  # stop polling after 5s if no message
        )
        
        while True:
            elapsed = time.time() - start_time
            log.info(f"Trying to find messages in kafka topic {topic} after {elapsed} seconds...")
            if elapsed > max_timeout_seconds:
                raise TimeoutError(f"No message recieved on topic {topic} after {max_timeout_seconds} seconds")

            messages = consumer.poll(timeout_ms=5000)
            if messages:
                log.info(f"✅ Messages were found on topic {topic}!")
                
                return f"messages were found!"
            
            time.sleep(poll_interval)
    
    @task
    def models_and_colors_create():
        run_command("dims/models_and_colors.py", detached=False, minio=True)
    
    @task
    def cars_create():
        run_command("dims/cars.py", detached=False, minio=True)
    
    @task
    #To kill, run this command in bash: docker exec dev_env pkill -f /home/developer/projects/spark-course-python/spark_course_python/project/data_generator.py
    def data_generator():
        run_command("data_generator.py", detached=True, minio=True)
    
    @task
    #To kill, run this command in bash: docker exec dev_env pkill -f /home/developer/projects/spark-course-python/spark_course_python/project/data_enrichment.py
    def data_enrichment():
        run_command("data_enrichment.py", detached=True)
        
    models_and_colors_create() >> cars_create() >> data_generator() >> wait_task(10) >> wait_for_message("sensors-sample") >> data_enrichment()

dag = pyspark_project_pipeline()