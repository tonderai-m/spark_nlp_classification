from pyspark.sql import SparkSession


def create_session():
    """
    Make sure you setup the config very well to the installed version of pyspark-npl
    replace 'com.johnsnowlabs.nlp:spark-nlp_2.12:X.X.X' with 'com.johnsnowlabs.nlp:spark-nlp_2.12:5.0.2'
    since my installed version was 5.0.2
    """
    spark = SparkSession \
        .builder \
        .master("local")\
        .appName("Python Spark SQL basic example") \
        .config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:5.0.2") \
        .getOrCreate()
    return spark

def read_data(spark):

    schema = "review_date string, \
              handle string, \
              rating string, \
              helpfulness_rating string, \
              review string, \
              "
    df = spark.read.options(header=True,schema=schema).csv("data")
    df = df.select("review", "rating")
    return df