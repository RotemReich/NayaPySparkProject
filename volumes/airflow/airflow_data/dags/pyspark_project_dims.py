from airflow.decorators import dag, task
#from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
import subprocess
from datetime import datetime
import time
import os
from kafka import KafkaConsumer


log = LoggingMixin().log

@dag(
    dag_id = "pyspark_project_pipline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["spark", "pyspark", "project", "Rotem Reich", "Avi Yashar", "Pipeline", "Init"]
)

def pyspark_project_pipeline():
    
    container_name = "dev_env"
    python_path = "/usr/bin/python3"
    path = "/home/developer/projects/spark-course-python/spark_course_python/project/"
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_token = os.getenv("AWS_SESSION_TOKEN")
    course_broker = "course-kafka:9092"
    
    def run_command(file: str, detached: bool = False, minio: bool = False):
        base_cmd = f"docker exec"
        
        if detached:
           base_cmd += " -d"
        
        if minio:
            if aws_access_key and aws_secret_key:
                base_cmd += f" -e AWS_ACCESS_KEY_ID={aws_access_key} -e AWS_SECRET_ACCESS_KEY={aws_secret_key}"
            else:
                raise ValueError("Missing AWS credentials in environment")
            
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
    
    def create_kafka_topic(topic_name: str):
        command_topics_list = f"docker exec course-kafka kafka-topics.sh --list --bootstrap-server {course_broker}"
        command = f"""docker exec course-kafka kafka-topics.sh \\
                    --create \\
                    --topic {topic_name} \\
                    --bootstrap-server {course_broker} \\
                    --partitions 1 \\
                    --replication-factor 1
                    """
        try:
            topics_list = subprocess.run(command_topics_list, shell=True, check=True, capture_output=True, text=True)
            existing_topics = [t.strip() for t in topics_list.stdout.splitlines()]
            
            if topic_name in existing_topics:
                log.info(f"Topic '{topic_name}' already exists\nNo topic was created.")
                return f"Topic '{topic_name}' already exists"
                        
            result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
            log.info("STDOUT: %s", result.stdout)
            log.info("STDERR: %s", result.stderr)
            log.info(f"Topic '{topic_name}' was created!", "="*100, "\n\n")
            return f"Topic '{topic_name}' was created"
        except subprocess.CalledProcessError as e:
            log.error("Error running '%s': returncode=%s", topic_name, e.returncode)
            log.error("STDOUT:\n%s", e.stdout)
            log.error("STDERR:\n%s", e.stderr)
            raise
    
    def wait_task(seconds: int = 60):
        print(f">>>Waiting for {seconds} seconds...")
        time.sleep(seconds)
        print(f">>>{seconds} seconds are over!")
        

    def wait_for_message(topic: str):
        max_timeout_seconds = 90
        poll_interval = 10
        start_time = time.time()
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers = course_broker,
            auto_offset_reset = "earliest",
            enable_auto_commit = False,
            #group_id = "airflow-wait-group",  # use a fixed group to always get earliest
            consumer_timeout_ms = 1000*poll_interval,  # stop polling after 10s if no message
        )
        
        while True:
            elapsed = time.time() - start_time
            log.info(f"Trying to find messages in kafka topic {topic} after {elapsed} seconds...")
            if elapsed > max_timeout_seconds:
                log.error(f"No message recieved on topic {topic} after {max_timeout_seconds} seconds.")
                raise TimeoutError(f"No message recieved on topic {topic} after {max_timeout_seconds} seconds.")

            messages = consumer.poll(timeout_ms = 1000*poll_interval)
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
    def create_topic_sensors_sample():
        create_kafka_topic("sensors-sample")
    
    @task
    def data_generator():
        run_command("data_generator.py", detached=True, minio=True)
    
    @task
    def wait_30_post_data_generator():
        wait_task(30)
        
    @task
    def wait_for_message_sensors_sample():
        wait_for_message("sensors-sample")
    
    @task
    def create_topic_samples_enriched():
        create_kafka_topic("samples-enriched")
    
    @task
    def data_enrichment():
        run_command("data_enrichment.py", detached=True)
    
    @task
    def wait_30_post_data_enrichment():
        wait_task(30)
        
    @task
    def wait_for_message_samples_enriched():
        wait_for_message("samples-enriched")
    
    (
        models_and_colors_create() >>
        cars_create() >>
        create_topic_sensors_sample() >>
        data_generator() >>
        wait_30_post_data_generator() >>
        wait_for_message_sensors_sample() >>
        create_topic_samples_enriched() >>
        data_enrichment() >>
        wait_30_post_data_enrichment() >>
        wait_for_message_samples_enriched()
    )    

dag = pyspark_project_pipeline()