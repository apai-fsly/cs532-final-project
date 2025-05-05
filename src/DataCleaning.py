import os,time
from pyspark.sql.functions import col,trim, initcap, split, when, array, lit,regexp_replace,row_number,concat_ws
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window
from CommonHelper import resolve_path, analyze_nulls, create_spark_session

#Load IMDb datasets from TSV files into Spark DataFrames
def load_data(spark, dataset_path):

    dataset_df = spark.read.option("sep", "\t") \
                          .option("header", "true") \
                          .option("nullValue", "\\N") \
                          .csv(dataset_path)
    
    return dataset_df



#Name.basics file
#Accept rows where primaryName is not null or empty and nconst is not null or empty
#Drop duplicates based on nconst and trim primaryName
#Based on the null analysis, we have decided to drop birthYear and deathYear due to high null counts. 
#Convert primaryProfession and knownForTitles to arrays of strings. Replace null for PrimaryProfession with Unknown and for knownForTitles with empty array.
def clean_name_basics(df):
    """Clean the name.basics data"""   
    return df.filter(col("nconst").isNotNull() & (col("nconst") != "")) \
             .filter(col("primaryName").isNotNull() & (col("primaryName") != "")) \
             .withColumn("primaryName", trim(col("primaryName"))) \
             .withColumn("primaryProfession", 
                        when(col("primaryProfession").isNull() | (col("primaryProfession") == ""), 
                             array(lit("Unknown")))
                        .otherwise(split(col("primaryProfession"), ","))) \
             .withColumn("knownForTitles", 
                        when(col("knownForTitles").isNull() | (col("knownForTitles") == ""), 
                             array())
                        .otherwise(split(col("knownForTitles"), ","))) \
             .drop("birthYear", "deathYear") \
             .dropDuplicates(["nconst"])


# Title.akas file
#Accept rows where titleId is not null or empty, title is not null or empty, and ordering is valid
#Convert ordering to integer and trim title
#Replace null for region with unknown and for types with empty array
#Convert types to an array of strings. Replace null for types with empty array.
def clean_title_akas(df):
    """Clean the title.akas data"""
    return df.filter(col("titleId").isNotNull() & (col("titleId") != "")) \
             .filter(col("title").isNotNull() & (col("title") != "")) \
             .withColumn("ordering", col("ordering").cast(IntegerType())) \
             .withColumn("title", trim(col("title"))) \
             .withColumn("region", 
                        when(col("region").isNull() | (col("region") == ""), "unknown")
                        .otherwise(col("region"))) \
             .withColumn("types", 
                        when(col("types").isNull() | (col("types") == ""), None)
                        .otherwise(split(col("types"), ","))) \
             .drop("attributes") \
             .dropDuplicates(["titleId", "ordering"])


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
          .filter(col("startYear").isNotNull())  # Simplified null check
          .filter(col("startYear").cast(IntegerType()).isNotNull())
          .filter((col("startYear") >= 1900) & (col("startYear") <= 2025))
          .dropDuplicates(["tconst"])
          .drop("endYear")
          .withColumn("primaryTitle", trim(col("primaryTitle")))
          .withColumn("primaryTitle", initcap(col("primaryTitle")))
    )


# Title.crew file
#Accept rows where tconst is not null or empty
#Convert directors and writers to arrays of strings. Replace null for directors with empty array and for writers with empty array.
def clean_title_crew(df):
    """Clean the title.crew data"""
    return df.filter(col("tconst").isNotNull() & (col("tconst") != "")) \
             .dropDuplicates(["tconst"]) \
             .withColumn("directors", 
                        when(col("directors").isNull() | (col("directors") == ""), array())
                        .otherwise(split(col("directors"), ","))) \
             .withColumn("writers", 
                        when(col("writers").isNull() | (col("writers") == ""), array())
                        .otherwise(split(col("writers"), ",")))


# Title.episode file
#Accept rows where tconst is not null or empty, parentTconst is not null or empty, seasonNumber is valid, and episodeNumber is valid
#Convert seasonNumber and episodeNumber to integers
def clean_title_episode(df):
    """Clean the title.episode data"""
    return df.filter(col("tconst").isNotNull() & (col("tconst") != "")) \
             .filter(col("parentTconst").isNotNull() & (col("parentTconst") != "")) \
             .filter(col("seasonNumber").isNotNull()) \
             .filter(col("episodeNumber").isNotNull()) \
             .withColumn("seasonNumber", col("seasonNumber").cast(IntegerType())) \
             .withColumn("episodeNumber", col("episodeNumber").cast(IntegerType())) \
             .dropDuplicates(["tconst"])


# Title.principals file
#Accept rows where tconst is not null or empty, nconst is not null or empty, and category is not null or empty
#Convert ordering to integer and trim category
#Replace null for job with empty string and for characters with empty array
#Convert characters to an array of strings. Replace null for characters with empty array.
def clean_title_principals(df):
    """Clean the title.principals data"""
    return df.filter(col("tconst").isNotNull() & (col("tconst") != "")) \
             .filter(col("nconst").isNotNull() & (col("nconst") != "")) \
             .filter(col("category").isNotNull() & (col("category") != "")) \
             .withColumn("ordering", col("ordering").cast(IntegerType())) \
             .withColumn("category", trim(col("category"))) \
             .withColumn("job", 
                        when(col("job").isNull() | (col("job") == ""), None)
                        .otherwise(trim(col("job")))) \
             .withColumn("characters", 
                        when(col("characters").isNull() | (col("characters") == ""), None)
                        .otherwise(split(regexp_replace(col("characters"), "[\\[\\]\"]", ""), ","))) \
             .dropDuplicates(["tconst", "nconst", "ordering"])


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
             .withColumnRenamed("averageRating", "movieRating")\
             .dropDuplicates(["tconst"])




# Method to load and clean all datasets
# This method will load all the datasets from the specified directory, clean them using the appropriate methods, and return a dictionary of cleaned DataFrames.
def clean_datasets(spark, data_dir, analyze=False,save_to_parquet=True):
    """
    Loads and cleans all IMDb datasets from a directory.
    
    Args:
        spark: Active SparkSession
        data_dir: Directory containing the IMDb dataset files
        analyze: Whether to analyze nulls in datasets (default: False)
        save_to_parquet: Whether to save cleaned datasets to parquet (default: True)
        
    Returns:
        Dictionary of cleaned DataFrames
    """
    # Define file paths
    name_basics_path = f"{data_dir}/name.basics.tsv"
    akas_path = f"{data_dir}/title.akas.tsv"
    basics_path = f"{data_dir}/title.basics.tsv"
    crew_path = f"{data_dir}/title.crew.tsv"
    episode_path = f"{data_dir}/title.episode.tsv"
    principals_path = f"{data_dir}/title.principals.tsv"
    ratings_path = f"{data_dir}/title.ratings.tsv"
    
    # Define datasets to process with their cleaning functions
    datasets_to_process = [
    {"name": "title_basics", "path": basics_path, "clean_func": clean_title_basics},
    {"name": "title_ratings", "path": ratings_path, "clean_func": clean_title_ratings},
    {"name": "title_akas", "path": akas_path, "clean_func": clean_title_akas},
    {"name": "title_crew", "path": crew_path, "clean_func": clean_title_crew},
    {"name": "title_principals", "path": principals_path, "clean_func": clean_title_principals},
    {"name": "name_basics", "path": name_basics_path, "clean_func": clean_name_basics},
    {"name": "title_episode", "path": episode_path, "clean_func": clean_title_episode},
  ]   
    
    cleaned_dfs = {}
    

    # Create parquet directory if saving is enabled
    parquet_dir = None
    if save_to_parquet:
        parquet_dir = resolve_path("./data/cleaned_datasets")
        os.makedirs(parquet_dir, exist_ok=True)
        print(f"Will save cleaned datasets to: {parquet_dir}")
    

    for dataset in datasets_to_process:
        try:
            print(f"\nProcessing {dataset['name']}...")
            
            # Load data
            df = spark.read.option("sep", "\t") \
                          .option("header", "true") \
                          .option("nullValue", "\\N") \
                          .csv(dataset['path'])
            
            # Analyze nulls if requested
            if analyze:
                print(f"Analyzing nulls in {dataset['name']}...")
                analyze_nulls(df)

            # Clean data
            print(f"Cleaning {dataset['name']}...")
            cleaned_df = dataset['clean_func'](df)
            
            # Save reference to cleaned DataFrame
            cleaned_dfs[dataset['name']] = cleaned_df

            # Count rows
            #row_count = cleaned_df.count()
            #print(f"Cleaned {dataset['name']} Row Count: {row_count}")


            # Save to parquet if requested
            if save_to_parquet:
                dataset_parquet_path = os.path.join(parquet_dir, dataset['name'])
                print(f"Saving cleaned {dataset['name']} to parquet at {dataset_parquet_path}...")
                cleaned_df.write.mode("overwrite").parquet(dataset_parquet_path)
            
            
            
        except Exception as e:
            print(f"Error processing {dataset['name']}: {e}")
    
    return cleaned_dfs



#This method will join the cleaned datasets to create a master movie dataset
#We will join the cleaned datasets on tconst primarily and include the essebtial columns from each dataset
#We will also include the top actor and actress for each movie, as well as the directors
def create_master_table(cleaned_dfs):
    """
    Integrates cleaned IMDb datasets into a unified movie dataset.
    This is the primary data processing pipeline component for performance analysis.
    
    Args:
        spark: Active SparkSession
        cleaned_dfs: Dictionary of already-cleaned DataFrames
        
    Returns:
        Integrated movie DataFrame ready for database storage
    """
    print("Starting data integration process...")
    
    
    # Extract cleaned DataFrames
    # We are not taking Title.Episode into account as we are targetting movie anaylsis
    basics_df = cleaned_dfs.get("title_basics").alias("basics")
    ratings_df = cleaned_dfs.get("title_ratings").alias("ratings") 
    akas_df = cleaned_dfs.get("title_akas").alias("akas") 
    crew_df = cleaned_dfs.get("title_crew").alias("crew") 
    principals_df = cleaned_dfs.get("title_principals").alias("principals")

    
    
    # Starting with the basics DataFrame
    print("Integrating basics data...")
    integrated_df = basics_df
    
    # Join with ratings if available
    if ratings_df:
        print("Joining with ratings data...")
        integrated_df = integrated_df.join(
            ratings_df,
            integrated_df["tconst"] == ratings_df["tconst"],
            "left"
        ).drop(ratings_df["tconst"])
    
    # Join with crew if available
    # This will add directors and writers to the movie data
    if crew_df:
        print("Joining with crew data...")
        integrated_df = integrated_df.join(
            crew_df,
            integrated_df["tconst"] == crew_df["tconst"],
            "left"
        ).drop(crew_df["tconst"])
    
    # Process and join with principals for top cast
    # We want to maintain one row per movie in the master table
    # So filtering the top actor and actress for each movie and pivoting them into separate columns
    if principals_df:
        print("Processing actors and actresses...")
        
        categories = ["actor", "actress"]
        
        for category in categories:
            print(f"Processing top {category}...")
            
            # Filter for the specific category
            category_df = principals_df.filter(col("principals.category") == category).alias("cat")
        
            # Create a window to select the top person in each category
            window_spec = Window.partitionBy("cat.tconst").orderBy("cat.ordering")
            
            # Get only the top-billed person 
            top_person_df = category_df.withColumn(
                "actorrank", row_number().over(window_spec)
            ).filter(col("actorrank") == 1).select(
                col("cat.tconst").alias("person_tconst"), 
                col("cat.nconst").alias(f"top_{category}")
            ).alias("person")
            
            # Join with main dataframe 
            if top_person_df.count() > 0:
                print(f"Joining with {category} data...")
                integrated_df = integrated_df.join(
                    top_person_df,
                    integrated_df["tconst"] == top_person_df["person_tconst"],
                    "left"
                ).drop("person_tconst")


    
    # Join with alternate titles (AKAs)
    # We will take the first alternate US based title for each movie
    if akas_df:
        print("Processing alternate titles...")
        # Get English AKAs as preferred
        english_akas = akas_df.filter(col("akas.region") == "US")
        

        # Take the first alternate title per movie
        window_spec = Window.partitionBy("titleId").orderBy("ordering")
        alt_titles = english_akas.withColumn(
            "alternateTitleRank", row_number().over(window_spec)
        ).filter(col("alternateTitleRank") == 1).select(
            col("titleId").alias("aka_titleId"),
            col("title").alias("alt_title")
        )#.alias("alt")

            
        # Join with main dataframe
        print("Joining with alternate titles...")
        integrated_df = integrated_df.join(
            alt_titles,
            integrated_df["tconst"] == alt_titles["aka_titleId"],
            "left"
        ).drop("aka_titleId")


    # Convert any array-type columns to strings before selecting final columns
    # This is the key step to make the DataFrame MySQL-compatible
    for field in integrated_df.schema.fields:
        field_name = field.name
        field_type = field.dataType
        if str(field_type).startswith("ArrayType"):
            integrated_df = integrated_df.withColumn(
                field_name, 
                concat_ws(",", col(field_name))
            )
    
    # Select and rename columns for final output
    print("Finalizing integrated dataset...")
    final_columns = [
        col("tconst").alias("movie_id"),
        col("primaryTitle").alias("title"),
        col("originalTitle").alias("original_title"),
        col("startYear").alias("year"),
        col("runtimeMinutes").alias("runtime"),
        col("genres").alias("genres"),
        col("title").alias("alternate_title"),
        col("movieRating").alias("rating"),
        col("numVotes").alias("votes"),

        # Additional columns from the join results
        col("top_actor").alias("lead_actor"),
        col("top_actress").alias("lead_actress"),
        col("directors").alias("directors")
    ]
    
    result_df = integrated_df.select(*final_columns)
    print(result_df.columns)

         
    print(f"Data integration completed")
    
    
    return result_df





# Main function to run the cleaning process and combine the dataset to create a master table
# This is for modular debugging and testing purposes
def main():
    # Create Spark session
    spark = create_spark_session()

    #Resolve the path to the data directory
    data_path = resolve_path("./data")
    
    # Define paths to cleaned datasets
    cleaned_dfs = clean_datasets(spark,data_path)
  
    # Run the integration process
    master_df = create_master_table(cleaned_dfs)
    
    # Show the master DataFrame (for debugging purposes)
    print("\nMaster DataFrame Schema:")
    master_df.printSchema()
    master_df.show(5, truncate=False)


    # Save the master DataFrame to a parquet file
    #output_path = resolve_path("./data/master_dataset/integrated_movie_data.parquet")
    #master_df.write.parquet(output_path, mode="overwrite")

    
    #print(f"Integrated dataset saved to {output_path}")
    
    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()