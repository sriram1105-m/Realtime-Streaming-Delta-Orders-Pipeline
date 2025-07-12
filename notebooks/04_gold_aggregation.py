from pyspark.sql.functions import col, count, sum, avg, to_date, datediff, max, row_number
from pyspark.sql.window import Window

# Paths
silver_path = "/Volumes/workspace/default/order_stream_volume/silver_orders"
gold_base_path = "/Volumes/workspace/default/order_stream_volume/gold_orders"

# Load Silver Data
silver_df = spark.read.format("delta").load(silver_path)
silver_df.createOrReplaceTempView("silver_orders")

#  1. Monthly Revenue & Order Metrics
monthly_revenue_df = spark.sql("""
    SELECT
        order_year,
        order_month,
        COUNT(order_id) AS total_orders,
        ROUND(SUM(order_amount), 2) AS total_revenue,
        ROUND(AVG(order_amount), 2) AS avg_order_value,
        ROUND(SUM(CASE WHEN order_status = 'returned' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS return_rate_pct,
        ROUND(SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS cancellation_rate_pct
    FROM silver_orders
    GROUP BY order_year, order_month
""")

monthly_revenue_df.write.mode("overwrite").format("delta") \
    .partitionBy("order_year", "order_month") \
    .save(f"{gold_base_path}/monthly_revenue")

# 2. RFM – Customer Segments
rfm_df = spark.sql("""
    SELECT
        customer_id,
        MAX(order_date) AS last_order_date,
        COUNT(order_id) AS frequency,
        ROUND(SUM(order_amount), 2) AS monetary_value,
        DATEDIFF(current_date(), MAX(order_date)) AS recency_days
    FROM silver_orders
    GROUP BY customer_id
""")

rfm_df.write.mode("overwrite").format("delta") \
    .save(f"{gold_base_path}/rfm_segments")

#  3. Top Customers per Region per Month
# Add window spec for top 1 per region/month
window_spec = Window.partitionBy("region", "order_year", "order_month") \
                    .orderBy(col("order_amount").desc())

top_customers_df = silver_df.withColumn("rn", row_number().over(window_spec)) \
                            .filter(col("rn") == 1) \
                            .drop("rn")

top_customers_df.write.mode("overwrite").format("delta") \
    .partitionBy("region") \
    .save(f"{gold_base_path}/top_customers_region_month")

#  4. Daily KPI Table
daily_kpis_df = (
    silver_df
    .withColumn("order_day", to_date("order_date"))
    .groupBy("order_day", "region")
    .agg(
        count("*").alias("total_orders"),
        sum("order_amount").alias("total_revenue"),
        avg("order_amount").alias("avg_order_value"),
        sum(col("is_high_value_order").cast("int")).alias("high_value_order_count")
    )
)

daily_kpis_df.write.mode("overwrite").format("delta") \
    .partitionBy("order_day", "region") \
    .save(f"{gold_base_path}/daily_kpis")

print(" All Gold tables written successfully.")
