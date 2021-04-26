from pyspark import SparkConf
from pyspark.sql import SparkSession
import logging
from helpers.log.Logger import setup_logging
setup_logging()

class BuildSparkSession:
    def __init__(self):
        """
        This one assigns default memory and cores to driver and executors
        """
        self.logger=logging.getLogger(__name__)
        self.driver_memory = "1g"
        self.driver_cores = "1"
        self.executor_cores = "1"
        self.executor_memory = "1g"


    def get_spark_session(self, driver_memory="1g", driver_cores="1", executor_cores="1", executor_memory="1g"):
        """Get the Spark Session with a given config
        :return : a spark session
        """
        #
        self.driver_memory = driver_memory
        self.driver_cores = driver_cores
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        # we can add more config setting based on the requirement
        self.logger.info("Allocation driver_cores:{},driver_memory:{},executor_cores:{},executor_memory:{}".format(self.driver_cores,self.driver_memory,self.executor_cores,self.executor_memory))
        conf = SparkConf()
        conf.set("spark.default.parallelism", "1")
        conf.set("spark.sql.shuffle.partitions", "1")
        conf.set("spark.driver.cores", driver_cores)
        conf.set("spark.driver.memory", driver_memory)
        conf.set("spark.executor.cores", executor_cores)
        conf.set("spark.executor.memory", executor_memory)

        # Get spark session by using the config build above
        try:
            sql_context = SparkSession.builder.config(conf=conf)
            sparksession = sql_context.enableHiveSupport().getOrCreate()
        except Exception as e:
            self.logger.error("Spark Application failed to initiate the application")
            exit(1)

        sparksession.sparkContext.setLogLevel("ERROR")

        self.logger.info("Spark Session successfully created")

        spark_app_id = sparksession._sc.applicationId
        spark_app_name = sparksession._sc.appName

        self.logger.info("Spark Application Name:{}".format(spark_app_name))
        self.logger.info("Spark Application Id:{}".format(spark_app_id))

        return sparksession
