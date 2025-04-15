from pyspark.sql import SparkSession
from pyspark.sql.functions import col,trim, initcap
from pyspark.sql.types import DoubleType, IntegerType


#Create a Spark session with specific configurations
def create_spark_session():
    """Create and return a Spark session"""
    return SparkSession.builder \
        .appName("Data Cleaning and Storage") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .getOrCreate()

#Load IMDb datasets from TSV files into Spark DataFrames
def load_data(spark, basics_path, ratings_path):

    basics_df = spark.read.option("sep", "\t") \
                          .option("header", "true") \
                          .option("nullValue", "\\N") \
                          .csv(basics_path)

    ratings_df = spark.read.option("sep", "\t") \
                           .option("header", "true") \
                           .option("nullValue", "\\N") \
                           .csv(ratings_path)
    
    return basics_df, ratings_df



#Title.basics file
#Accept rows where titleType is "movie",tconst is not null or empty, primaryTitle is not null or empty, and startYear is valid
#Take all columns except endYear as thats for a series
#Drop duplicates based on tconst and trim primaryTitle

def clean_title_basics(df):
    """Clean the title.basics data"""
    return (
        df.filter(col("titleType") == "movie")
          .filter(col("tconst").isNotNull() & (col("tconst") != ""))
          .filter(col("primaryTitle").isNotNull() & (col("primaryTitle") != ""))
          .filter(col("startYear").isNotNull() & (col("startYear") != "\\N"))
          .filter(col("startYear").cast(IntegerType()).isNotNull())
          .filter((col("startYear") >= 1900) & (col("startYear") <= 2025))
          .dropDuplicates(["tconst"])
          .drop("endYear")
          .withColumn("primaryTitle", trim(col("primaryTitle")))
          .withColumn("primaryTitle", initcap(col("primaryTitle")))
    )


#Title.ratings file
#Accept rows where averageRating is valid and numVotes is greater than 0
#Rename averageRating to movieRating
#Convert averageRating to double and numVotes to integer

def clean_title_ratings(df):
    """Clean the title.ratings data"""
    return df.filter(col("averageRating").isNotNull()) \
             .filter(col("numVotes").isNotNull()) \
             .filter((col("averageRating") >= 0) & (col("averageRating") <= 10)) \
             .filter(col("numVotes") > 0) \
             .withColumn("averageRating", col("averageRating").cast(DoubleType())) \
             .withColumn("numVotes", col("numVotes").cast(IntegerType())) \
             .withColumnRenamed("averageRating", "movieRating")


def main():
    spark = create_spark_session()
    
    # Define paths to files
    basics_path = "../data/title.basics.tsv"
    ratings_path = "../data/title.ratings.tsv"
    
    # Load data
    print("Loading data...")
    basics_df, ratings_df = load_data(spark, basics_path, ratings_path)
    
    # Clean data
    print("Cleaning data...")
    cleaned_basics = clean_title_basics(basics_df).orderBy("tconst")
    cleaned_ratings = clean_title_ratings(ratings_df).orderBy("tconst")


    #Force Spark to do lazy evaluation
    # This will trigger the transformations and actions

    rowCount =cleaned_basics.count()
    print(f"Cleaned Title Basics Row Count: {rowCount}")
    
    rowCount = cleaned_ratings.count()
    print(f"Cleaned Title Ratings Row Count:{rowCount}")
    

    #Print the top 10 rows of the cleaned data
    print("Cleaned Title Basics Data:")
    cleaned_basics.show(10, truncate=False)

    print("Cleaned Title Ratings Data:")
    cleaned_ratings.show(10, truncate=False)
    

    spark.stop()

if __name__ == "__main__":
    main()