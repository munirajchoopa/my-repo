import logging.config
from pyspark.sql import functions as f
from helpers.log.Logger import setup_logging

setup_logging()

def landing_to_cleansed(spark,source_path :str,cleansed_path:str):
    """
    This function get the data from landing location
    and clean the data for best structure to utilized by downstreams
    :param spark:spark Session 
    :param source_path: landing path
    :param cleansed_path: cleansed path where data lands after cleansing
    :return: dataframe
    """
    logger=logging.getLogger(__name__)
    logger.info("Starting reading the data from source path:{}".format(source_path))
    try:
            df= spark.read.json(source_path)
    except Exception as e:
        logger.error("Reading data from lanidng path failed",exc_info=True)

    logger.info("No of records processing:{}".format(df.count()))
    logger.info("Started performing filter of records based on ingredients")

    """
    Filter the records which has beef as ingredient
    """
    df = df.filter(f.lower(df.ingredients).contains('beef'))

    logger.info("Formatting prep and cook time to be used for downstreams ")


    df = df.withColumn('cooktime_cleansed', f.translate(df.cookTime, 'PT', '')).withColumn('preptime_cleansed',
                                                                                                        f.translate(
                                                                                                            df.prepTime,
                                                                                                            'PT', ''))
    df = df.drop('prepTime').drop('cookTime')
    df = df.withColumnRenamed('cooktime_cleansed','cooktime').withColumnRenamed('preptime_cleansed','preptime')

    logger.info("Drop of unused columns completed ")
    logger.info("Calculating cooking time and prep time in minutes")

    """
    Here we remove PT string and convert the string time which has H and M to spark recognized time format and calculate to minutes
    below data frame will have two extra columns cook_time_in_mins,prep_time_in_mins which has minute values
    We dont tocuh existing data which has with H and M letters ,since this data may be used by some other reports or KPI's    
    """
    try:
        df = df.withColumn('cook_time_in_mins', f.when(
        df.cooktime.contains('H') & df.cooktime.contains('M'), f.minute(
            f.date_format(f.regexp_replace(f.regexp_replace(df.cooktime, 'H', ':'), 'M', ''),
                          'H:mm')) + f.hour(
            f.date_format(f.regexp_replace(f.regexp_replace(df.cooktime, 'H', ':'), 'M', ''),
                          'H:mm')) * 60).when(df.cooktime.contains('H'),
                                              (f.regexp_replace(df.cooktime, 'H', '') * 60).cast(
                                                  "int")).otherwise(
        f.regexp_replace(df.cooktime, 'M', ''))).withColumn('prep_time_in_mins', f.when(
        df.preptime.contains('H') & df.preptime.contains('M'), f.minute(
            f.date_format(f.regexp_replace(f.regexp_replace(df.preptime, 'H', ':'), 'M', ''),
                          'H:mm')) + f.hour(
            f.date_format(f.regexp_replace(f.regexp_replace(df.preptime, 'H', ':'), 'M', ''),
                          'H:mm')) * 60).when(df.preptime.contains('H'),
                                              (f.regexp_replace(df.preptime, 'H', '') * 60).cast(
                                                  "int")).otherwise(
        f.regexp_replace(df.preptime, 'M', '')))
    except Exception as e:
        logger.error("Issue while deriving timings in minutes,aborting",exc_info=True)
        return 1

    logger.info("Started loading the cleansed data to cleansed layer:{}".format(cleansed_path))

    try:
        df.repartition(1).write.mode('overwrite').option("mapreduce.fileoutputcommitter.marksuccessfuljobs",
                                                                 "false").json(cleansed_path)
        logger.info("Completed loading the data to cleansed path")
        return 0
    except Exception as e:
        logger.error("While loading data to cleansed,please check", exc_info=True)
        return 1



