import time

logger = None
job_start_time = None


def start_logging(spark, job_name):
    global job_start_time
    job_start_time = time.perf_counter()
    
    spark_log4j = spark.sparkContext._jvm.org.apache.log4j
    global logger 
    logger = spark_log4j.LogManager.getLogger("pyspark_logger")
    
    logger.info(f"Starting logger for {job_name}")

    
def log_debug_message(message):
    logger.debug(message)
    
    
def log_informational_message(message):
    logger.info(message)
    

def log_warning_message(message):
    logger.warn(message)
    

def log_error_message(message):
    logger.error(message)
    

def stop_logging(job_name):
    job_end_time = time.perf_counter()
    duration = str(job_end_time - job_start_time)
    logger.info(f"Stopping logger for {job_name}; total duration {duration}")