import pandas as pd
import seaborn as sns
import datetime as dt
import matplotlib.pyplot as plt
import os
import time
import helper
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *
from helper import create_immigration_table, create_date_table, create_temperature_table, create_demographics_table, extract_lookup, check_data_quality

def main():
    
    print("Starting ETL process...")
    spark = SparkSession\
                .builder.\
                config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
                .enableHiveSupport()\
                .getOrCreate()
    ### 1. Load input datasets to Spark and Pandas
    df_immigration = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')
    df_temperature = pd.read_csv('../../data2/GlobalLandTemperaturesByCity.csv')
    df_demographics_spark = spark.read.csv('us-cities-demographics.csv', inferSchema=True, header=True, sep=';')
    
    ### 2. Clean datasets and create fact and dimension tables
    immigration_fact_table = create_immigration_table(df_immigration, spark)
    date_dim_table = create_date_table(df_immigration)
    temperature_dim_table = create_temperature_table(df_temperature, spark)
    demographics_dim_table = create_demographics_table(df_demographics_spark)
    
    ### 3. Run data quality checks
    all_dfs = {
        'immigration fact table': immigration_fact_table,
        'temperature dimension table': temperature_dim_table,
        'date dimension table': date_dim_table,
        'demographics dimension table': demographics_dim_table
    }
    for table_name, df in all_dfs.items():
        check_data_quality(df, table_name)
    
    print("ETL process has completed.")
    
    
if __name__ == "__main__":
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))