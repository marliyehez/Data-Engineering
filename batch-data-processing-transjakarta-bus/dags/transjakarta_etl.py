from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, concat, concat_ws, create_map, lit
from itertools import chain
import gcsfs


# Set the GCS bucket and folder path
BUCKET = "transjakarta-data"
FOLDER_PATH = "raw-data"

# Create a Spark session
spark = SparkSession.builder.appName("TransjakartaETL").getOrCreate()

# Create a file system object
fs = gcsfs.GCSFileSystem()

# List all the csv files in the folder
files = fs.ls(f"{BUCKET}/{FOLDER_PATH}")
csv_files = [f"gs://{file}" for file in files if file.endswith('.csv')]


# Initialize an empty list to store DataFrames
dataframes = []

# Loop through the CSV files and read them into DataFrames
for filename in csv_files:
    df = spark.read.csv(filename, header=True, inferSchema=True)
    dataframes.append(df)

# Merge the DataFrames using union()
fact_table = dataframes[0]
for df in dataframes[1:]:
    fact_table = fact_table.union(df)

# Sort based on date
fact_table = fact_table.orderBy(col("tahun").asc(), col("bulan").asc())

# Dimension tables
date_dim = fact_table.select("bulan", "tahun") \
    .dropDuplicates() \
    .orderBy(col("tahun").asc(), col("bulan").asc()) \
    .withColumn("id", monotonically_increasing_id()) \
    .select("id", "bulan", "tahun")

route_dim = fact_table.select("kode_trayek", "trayek", "jenis") \
    .dropDuplicates() \
    .withColumn("id", monotonically_increasing_id()) \
    .select("id", "kode_trayek", "trayek", "jenis")

vehicle_dim = route_dim.select("jenis") \
    .dropDuplicates() \
    .withColumn("id", monotonically_increasing_id()) \
    .select("id", "jenis")


# Foreign keys dict
vehicle_dict = vehicle_dim.select("jenis", "id").rdd.collectAsMap()
route_dict = route_dim.select("kode_trayek", "id").rdd.collectAsMap()
date_dict = date_dim.withColumn(
    "bulan_tahun",
    concat_ws(
        "_",
        col("bulan"),
        col("tahun")
    )
).select("bulan_tahun", "id").rdd.collectAsMap()

# Creating foregin keys
mapping_expr = create_map([lit(x) for x in chain(*vehicle_dict.items())])
route_dim = route_dim.withColumn("vehicle_fk", mapping_expr.__getitem__(col("jenis")))

mapping_expr = create_map([lit(x) for x in chain(*route_dict.items())])
fact_table = fact_table.withColumn("route_fk", mapping_expr.__getitem__(col("kode_trayek")))

mapping_expr = create_map([lit(x) for x in chain(*date_dict.items())])
fact_table = fact_table.withColumn(
    "bulan_tahun",
    concat_ws(
        "_",
        col("bulan"),
        col("tahun")
    )
).withColumn("date_fk", mapping_expr.__getitem__(col("bulan_tahun")))


# Drop columns
cols = ["tahun", "bulan", "jenis", "kode_trayek", "trayek", "bulan_tahun"]
fact_table = fact_table.drop(*cols)
route_dim = route_dim.drop("jenis")


# Define the BigQuery dataset and table name
PROJECT_ID = "golden-union-392713"
DATASET_NAME = "transjakarta"
tables_list = [
    (fact_table,'fact_table'),
    (route_dim, 'route_dim'),
    (date_dim, 'date_dim'),
    (vehicle_dim, 'vehicle_dim'),
]


# Load the DataFrame to BigQuery
try:
    for df, TABLE_NAME in tables_list:
        df.write \
            .format("bigquery") \
            .option("table", f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}") \
            .option("temporaryGcsBucket", "transjakarta-data") \
            .mode("overwrite") \
            .save()
except Exception as e:
    print(e)

# Stop the Spark session when done
spark.stop()
