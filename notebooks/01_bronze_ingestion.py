from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

# Input JSON path
input_path = "/Volumes/workspace/default/order_stream_volume/input_data"

# Bronze Delta Table output path
bronze_output_path = "/Volumes/workspace/default/order_stream_volume/bronze_orders"

# Checkpoint location for Bronze
bronze_checkpoint_path = "/Volumes/workspace/default/order_stream_volume/checkpoints/bronze_orders"

# Defining Schema
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("region", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_date", StringType(), True)
])

# STep 2: Read from JSON, Transform, and Parse Timestamp
from pyspark.sql.functions import to_timestamp

# Read from JSON Stream
bronze_orders_df = (
    spark.readStream
    .schema(order_schema)
    .option("maxFilesPerTrigger", 1)
    .json(input_path)
)

# COnvert order_date from string to timestamp
bronze_transformed_df = bronze_orders_df.withColumn(
    "order_date", to_timestamp("order_date", "yyyy-MM-dd HH:mm:ss")
)

# Step 3: Write Bronze Stream to Delta Table

bronze_path = "/Volumes/workspace/default/order_stream_volume/bronze_orders"
bronze_checkpoint = "/Volumes/workspace/default/order_stream_volume/checkpoints/bronze"

# Write stream to Bronze Delta
bronze_query = (
    bronze_transformed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", bronze_checkpoint)
    .trigger(once = True)
    .start(bronze_path)
)


# Read data from Bronze Delta
bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df)
