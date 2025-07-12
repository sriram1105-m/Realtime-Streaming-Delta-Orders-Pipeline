from pyspark.sql.functions import col, count, when

# Paths
quarantine_path = "/Volumes/workspace/default/order_stream_volume/quarantine/silver_orders"

# Load quarantined records (non-streaming)
quarantine_df = spark.read.format("delta").load(quarantine_path)

# Show sample invalid records
print("Sample Quarantined Records:")
quarantine_df.show(truncate=False)

# Count total invalid rows
total_invalid = quarantine_df.count()
print(f"Total Invalid Records: {total_invalid}")

# Analyze why rows might be invalid (if no 'reason' column is available, infer from fields)
quarantine_df.createOrReplaceTempView("quarantine")

# Breakdown: Missing or invalid fields
spark.sql("""
    SELECT 
        COUNT(*) AS count,
        CASE
            WHEN region IS NULL THEN 'Missing Region'
            WHEN region NOT IN ('North', 'South', 'East', 'West') THEN 'Invalid Region'
            WHEN order_status NOT IN ('delivered', 'cancelled', 'returned') THEN 'Invalid Status'
            WHEN order_date IS NULL THEN 'Missing Order Date'
            ELSE 'Other Issues'
        END AS quarantine_reason
    FROM quarantine
    GROUP BY quarantine_reason
    ORDER BY count DESC
""").show(truncate=False)

# Optional: Group by region or status to find patterns
print(" Invalid Records by Region:")
quarantine_df.groupBy("region").count().orderBy("count", ascending=False).show()

print(" Invalid Records by Status:")
quarantine_df.groupBy("order_status").count().orderBy("count", ascending=False).show()
