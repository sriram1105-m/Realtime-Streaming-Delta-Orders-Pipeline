from pyspark.sql.functions import *
from pyspark.sql.types import *

bronze_path = "/Volumes/workspace/default/order_stream_volume/bronze_orders"
silver_path = "/Volumes/workspace/default/order_stream_volume/silver_orders"
silver_checkpoint_path = "/Volumes/workspace/default/order_stream_volume/checkpoints/silver_orders"
quarantine_path = "/Volumes/workspace/default/order_stream_volume/quarantine/silver_orders"

# Load bronze stream
bronze_df = spark.readStream.format("delta").load(bronze_path)

# Define allowed values
valid_regions = ["North", "South", "East", "West"]
valid_statuses = ["delivered", "cancelled", "returned"]

# Casting all fields
typed_df = (
    bronze_df
    .withColumn("order_id", col("order_id").cast("string"))
    .withColumn("customer_id", col("customer_id").cast("string"))
    .withColumn("order_date", to_timestamp("order_date"))
    .withColumn("order_amount", col("order_amount").cast("double"))
    .withColumn("region", col("region").cast("string"))
    .withColumn("order_status", col("order_status").cast("string"))
)

# Derive additional features
transformed_df = (
    typed_df
    .withColumn("order_month", month("order_date"))
    .withColumn("order_year", year("order_date"))
    .withColumn("order_dayofweek", date_format("order_date", "EEEE"))
    .withColumn("is_high_value_order", col("order_amount") > 300)
    .withColumn("ingestion_time", current_timestamp())
)

# Filter for valid records
valid_df = (
    transformed_df
    .filter(col("region").isin(valid_regions))
    .filter(col("order_status").isin(valid_statuses))
    .filter(col("order_date").isNotNull())
)

# Add validation flag
transformed_df = transformed_df.withColumn(
    "is_valid",
    (col("region").isin(valid_regions)) &
    (col("order_status").isin(valid_statuses)) &
    (col("order_date").isNotNull())
)

# Separate valid and invalid records
valid_df = transformed_df.filter(col("is_valid") == True).drop("is_valid")
invalid_df = transformed_df.filter(col("is_valid") == False).drop("is_valid")

# Write valid data to Silver table
valid_query = (
    valid_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", silver_checkpoint_path)
    .trigger(once=True)
    .start(silver_path)
)

# Write bad data to quarantine table
bad_query = (
    invalid_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", quarantine_checkpoint_path)
    .trigger(once=True)
    .start(quarantine_path)
)
