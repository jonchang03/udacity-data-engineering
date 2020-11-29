# Import Libraries
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import datetime as dt
import os
import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear, avg
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import *

    
def extract_lookup(path, value, cols):
    """Parse SAS file for the desired lookup table and returns as a dataframe. 

    Args:
        path (str): path to SAS file.
        value (str): SAS value to extract.
        columns (list): list of column names
    Return:
        DataFrame
    """
    txt = ''
    
    with open(path) as f:
        txt = f.read()
    
    txt = txt[txt.index(value):]
    txt = txt[:txt.index(';')]
    
    lines = txt.split('\n')[1:]
    keys = []
    values = []
    
    for line in lines:
        
        if '=' in line:
            k, v = line.split('=')
            k = k.strip()
            v = v.strip()

            if k[0] == "'":
                k = k[1:-1]

            if v[0] == "'":
                v = v[1:-1]

            keys.append(k)
            values.append(v)
        
            
    return pd.DataFrame(list(zip(keys,values)), 
                        columns=cols)



def view_missing_values(df):
    """
    Identify and visualize missing values for a given dataframe (Spark or pandas).
    """
    # create a dataframe with missing values count per column
    if type(df) == pyspark.sql.dataframe.DataFrame:
        nulls_df = df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns]).toPandas()
        nulls_df = pd.melt(nulls_df, var_name='cols', value_name='values')
        nulls_df['% missing values'] = 100*nulls_df['values'] / df.count()
    elif type(df) == pd.core.frame.DataFrame:
        nulls_df = pd.DataFrame(data= df.isnull().sum(), columns=['values'])
        nulls_df = nulls_df.reset_index()
        nulls_df.columns = ['cols', 'values']
        nulls_df['% missing values'] = 100*nulls_df['values']/df.shape[0]
        
    plt.rcdefaults()
    plt.figure(figsize=(10,5))
    ax = sns.barplot(x="cols", y="% missing values", data=nulls_df)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()
    return nulls_df



def create_immigration_table(df, 
                             spark,
                             lookup_path='I94_SAS_Labels_Descriptions.SAS', 
                             output_path="./output_data/immigration"):
    """ Create immigration fact table based off of the immigration events 
    from the I94 dataset.

    Args:
        df (Spark dataframe): input Spark dataframe with immigration data
        lookup_path (str): path to SAS data dictionary to generate country code loookup
        output_path (str): path to write the output dataframe to
    Return:
        DataFrame corresponding to date dimension table
    """
    # udf to convert arrival date to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)
    
    int_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 
                'i94mode', 'i94bir', 'i94visa','dtadfile', 'depdate']
    null_cols = ['occup', 'entdepu','insnum','visapost']
    other_cols = ['count', 'matflag', 'dtaddto', 'biryear', 'admnum']
    drop_cols = null_cols + other_cols
    
    for col_name in int_cols:
        df = df.withColumn(col_name, col(col_name).cast('integer'))        
    df_immigration = df.drop(*drop_cols)
    df_immigration = df_immigration.withColumn("arrdate", get_datetime(df_immigration.arrdate))\
                                    .withColumnRenamed('cicid','record_id') \
                                    .withColumnRenamed('i94res', 'country_code') \
                                    .withColumnRenamed('i94addr', 'state_code') 
    
    # join country code
    country_lookup = extract_lookup(lookup_path, 'i94cntyl', ['country_code', 'country'])
    country_lookup['country'] = country_lookup['country'].str.title()
    country_lookup = spark.createDataFrame(country_lookup)
    df_immigration = df_immigration.join(country_lookup, on='country_code', how='left')
    
    # write the fact table to parquet file
    partition_columns = ['i94yr', 'i94mon']
    df_immigration.write.parquet(output_path, 
                          partitionBy=partition_columns, 
                          mode="overwrite")
    return df_immigration



def create_date_table(df, output_path="./output_data/date"):
    """ Creates a date dimension table based off of the arrival dates from the 
    immigration events.

    Args:
        df (Spark dataframe): input Spark dataframe with demographics data
        output_path (str): path to write the output dataframe to
    Return:
        DataFrame corresponding to date dimension table
    """

    # udf to convert arrival date to datetime object
    get_datetime = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(x)).isoformat() if x else None)

    # use arrival date to create dataframe
    df_date = df.select(['arrdate']).withColumn("arrdate", get_datetime(df.arrdate)).distinct()
    df_date = df_date.withColumn('date_id', monotonically_increasing_id())
    df_date = df_date.withColumn('arrival_day', dayofmonth('arrdate'))
    df_date = df_date.withColumn('arrival_week', weekofyear('arrdate'))
    df_date = df_date.withColumn('arrival_month', month('arrdate'))
    df_date = df_date.withColumn('arrival_year', year('arrdate'))
    df_date = df_date.withColumn('arrival_weekday', dayofweek('arrdate'))
    

    # write the calendar dimension to parquet file
    partition_columns = ['arrival_year', 'arrival_month', 'arrival_week']
    df_date.write.parquet(output_path, partitionBy=partition_columns, mode="overwrite")

    return df_date



def create_temperature_table(df, spark, output_path="./output_data/temperature"):
    """ Creates a temperature dimension table based on the World Temperature dataset 
    from Kaggle. The average temperature data is aggregated at the country level.

    Args:
        df (Spark dataframe): input Pandas dataframe with temperature records
        output_path (str): path to write the output dataframe to
    Return:
        DataFrame corresponding to temperature dimension table
    """
    df_temperature = df[['Country', 'AverageTemperature']]\
                            .dropna(how='any')\
                            .groupby('Country')['AverageTemperature'].mean()\
                            .reset_index()
    df_temp = spark.createDataFrame(df_temperature)
    df_temp.repartition(1)\
          .write.parquet(output_path, mode="overwrite")
    return df_temp



## demographics dimension table
def create_demographics_table(df, output_path="./output_data/demographics"):
    """ Creates a demographics dimension table based on the US City Demographic Data.

    Args:
        df (Spark dataframe): input Spark dataframe with immigration events
        output_path (str): path to write the output dataframe to
    Return:
        DataFrame corresponding to demographics dimension table
    """
    # clean demographics dataset
    subset_cols = ['Male Population', 'Female Population', 'Number of Veterans',
                   'Foreign-born', 'Average Household Size']
    
    
    df_demographics = df.dropna(subset=subset_cols)
    
    df_demographics = df_demographics.withColumn('demographics_id', monotonically_increasing_id()) \
                                    .withColumnRenamed('Median Age','median_age') \
                                    .withColumnRenamed('Male Population', 'male_population') \
                                    .withColumnRenamed('Female Population', 'female_population') \
                                    .withColumnRenamed('Total Population', 'total_population') \
                                    .withColumnRenamed('Number of Veterans', 'number_of_veterans') \
                                    .withColumnRenamed('Foreign-born', 'foreign_born') \
                                    .withColumnRenamed('Average Household Size', 'average_household_size') \
                                    .withColumnRenamed('State Code', 'state_code') \
                                    .withColumnRenamed('City', 'city') \
                                    .withColumnRenamed('State', 'state') \
                                    .withColumnRenamed('Race', 'race') \
                                    .withColumnRenamed('Count', 'count')
    
    # write dimension to parquet file
    df_demographics.repartition(1)\
                   .write.parquet(output_path, mode="overwrite")
    
    return df_demographics

def check_data_quality(df, table_name):
    """ Run the following data quality checks for a given dataframe:
    1. records exist
    2. no duplicate records
    
    If any of the checks fail, then 
    
    Args:
        df (Spark dataframe): input Spark dataframe with immigration events
    Return:
        None
    """
    print(f'Starting data quality checks for {table_name}...')
    
    count = df.count()
    if count == 0:
        raise ValueError(f"Data quality check #1 failed: {table_name} has zero records!")
        
    if count != df.distinct().count():
        raise ValueError(f"Data quality check #2 failed: {table_name} has duplicate records!")
        
    print(f"All data quality checks have passed for: {table_name} which has {count} records!")