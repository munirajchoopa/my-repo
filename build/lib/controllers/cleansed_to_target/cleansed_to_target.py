import logging.config
from pyspark.sql import functions as f
from pyspark.sql.functions import col
from helpers.log.Logger import setup_logging

setup_logging()

def cleansed_to_target(spark,cleansed_path :str,target_path:str):
    """
    This function gets the data from cleansed layer and
    apply the transformations and load the data to target layer in required format
    :param spark: spark session
    :param cleansed_path: Data comes from this path to load to target
    :param target_path: Target location where we need to load
    :return: none
    """
    logger = logging.getLogger(__name__)
    logger.info("Starting reading the data from cleansed path:{}".format(cleansed_path))

    try:
            df= spark.read.json(cleansed_path)
            df = df.select(col("name"),col('cook_time_in_mins'),col('prep_time_in_mins'))
    except Exception as e:
        logger.error("Reading data from cleansed path failed",exc_info=True)
        exit(1)

    logger.info("No of records processing from cleansed data set:{}".format(df.count()))

    # Deriving total_cook_time based on formula total_cook_time + total_prep_time
    
    df = df.withColumn('total_cook_time', col('cook_time_in_mins') + col('prep_time_in_mins'))
    
    logger.info("Calulation of total Cook time completed ")

    # Deriving difficulty based on given formula
    
    df = df.withColumn('difficulty', f.when(col("total_cook_time") < "30", "easy").when(
        (col("total_cook_time") >= "30") & (col("total_cook_time") <= "60"), "medium").otherwise("hard"))
    
    logger.info("Finding the difficulty level based on total cook time ")

    # Finding the average cooking time group by difficulty
    
    df = df.select(col("difficulty"), col("total_cook_time")).groupBy(col("difficulty")).avg(
        "total_cook_time")
    
    logger.info("Finding the average cook time for difficulty level ")

    # Below logic  used  to convert minutes in to readable format of time ex: 1 hour 25 Minutes

    try:
        df = df.select(col("difficulty"), f.when(f.floor(col("avg(total_cook_time)")) > "60",
                                                                  f.concat(f.floor(col("avg(total_cook_time)") / 60),
                                                                           f.lit(' Hour '),
                                                                           f.floor(col("avg(total_cook_time)") % 60),
                                                                           f.lit(' Minutes'))).otherwise(
        f.concat(f.floor(col("avg(total_cook_time)")), f.lit(' Minutes'))).alias("avg_total_cooking_time"))
    except Exception as e :
        logger.Error("Issue while finding the average cook time for difficulty level ,please check",exc_info=True)
        exit(1)

    logger.info("Completed proccessing on finding difficulty levels on average cook time")

    logger.info("Started loading the data to target path in csv format: {}".format(target_path))
    
    try:
        df.repartition(1).write.mode('overwrite').option("mapreduce.fileoutputcommitter.marksuccessfuljobs",
                                                                 "false").option("header", "true").csv(target_path)
        logger.info("Completed loading the data to target path")
        return 0
    except Exception as e:
        logger.error("While loading data to target,please check", exc_info=True)
