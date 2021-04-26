import json
import pkg_resources
from helpers.proxy.SparkProxy import BuildSparkSession
import helpers.config
from controllers.landing_to_cleansed.landing_to_cleansed import *
from controllers.cleansed_to_target.cleansed_to_target import *
from py4j.java_gateway import  java_import

# Setup the logging configuration
setup_logging()
logger = logging.getLogger("derive_average_time.py")

# Read the variables from the parameter file

logger.info("Reading all the variables from config file")
json_config_file=pkg_resources.resource_filename(helpers.config.__name__, "parameters.json")
with open(json_config_file) as config_file:
    data = json.load(config_file)

landing_path = data['variables']['layers']['landing_zone']
cleansed_path = data['variables']['layers']['cleansed_zone']
target_path = data['variables']['layers']['target_zone']

logger.info("landing_path:{}".format(landing_path))
logger.info("cleansed_path:{}".format(cleansed_path))
logger.info("target_path:{}".format(target_path))

if __name__ == "__main__":

    """
    Main execution starts here and execution ends here
    """

    logger.info("starting to create spark session")
    try:
        sparksession=BuildSparkSession()
        # we can pass many parameters with dictionary if required
        spark=sparksession.get_spark_session(driver_cores="1",driver_memory="1g",executor_cores=1,executor_memory="1g")
    except Exception as e:
        logger.error("Spark Application failed to initiate the application")
        exit(1)
    logger.info("Initiating Spark session completed:{}".format(spark))
    logger.info("Starting cleansing job to clean data based on requirements")

    # Calling landing to cleasned job to perform cleansing functions

    return_cleansed = landing_to_cleansed(spark,landing_path,cleansed_path)

    logger.info("return_cd_cleansed:{}".format(return_cleansed))

    if return_cleansed != 0:
         logger.error("Landing to cleansed job failed,aborting the process.")
         spark.stop()
         exit(1)

    # Calling cleansing to target job to perform KPI driven requirements

    return_target = cleansed_to_target(spark, cleansed_path, target_path)

    logger.info("return_cd_target:{}".format(return_target))

    if return_target != 0:
         logger.error("Landing to cleansed job failed,aborting the process.")
         spark.stop()
         exit(1)

    logger.info("Starting to rename the file from part* to report.csv")

    # Below script used to rename the part files to required file name
    java_import(spark._jvm, 'org.apache.hadoop.fs.Path')
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    file = fs.globStatus(spark.sparkContext._jvm.Path(target_path + '\\' + 'part*.csv'))[0].getPath().getName()
    fs.rename(spark.sparkContext._jvm.Path(target_path +'\\' + file), spark.sparkContext._jvm.Path(target_path +'\\' + 'report.csv'))

    logger.info("Completed renaming the file ")
    logger.info("Stopping Spark Session")
    spark.stop()
    logger.info("Spark session closed")
    logger.info("Job Completed Successfully")