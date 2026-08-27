from pyspark.sql import functions as F


# BRONZE LAYER

base_path = "/mnt/ecommerce"

bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"

customers_path = f"{base_path}/source/customers.csv"
products_path = f"{base_path}/source/products.csv"
orders_path = f"{base_path}/source/orders.csv"
transactions_path = f"{base_path}/source/transactions.csv"


customers = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(customers_path)
)

products = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(products_path)
)

orders = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(orders_path)
)

transactions = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(transactions_path)
)


customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{bronze_path}/customers")

products.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{bronze_path}/products")

orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{bronze_path}/orders")

transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{bronze_path}/transactions")


# SILVER LAYER

customers = spark.read.format("delta").load(
    f"{bronze_path}/customers"
)

products = spark.read.format("delta").load(
    f"{bronze_path}/products"
)

orders = spark.read.format("delta").load(
    f"{bronze_path}/orders"
)

transactions = spark.read.format("delta").load(
    f"{bronze_path}/transactions"
)


customers_clean = (
    customers
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
    .withColumn(
        "customer_name",
        F.trim(F.col("customer_name"))
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "city",
        F.trim(F.col("city"))
    )
    .withColumn(
        "country",
        F.trim(F.col("country"))
    )
)

customers_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/customers")


products_clean = (
    products
    .dropDuplicates(["product_id"])
    .filter(F.col("product_id").isNotNull())
    .withColumn(
        "product_name",
        F.trim(F.col("product_name"))
    )
    .withColumn(
        "category",
        F.trim(F.col("category"))
    )
    .withColumn(
        "price",
        F.col("price").cast("double")
    )
    .filter(F.col("price") >= 0)
)

products_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/products")


orders_clean = (
    orders
    .dropDuplicates(["order_id"])
    .filter(
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull()
    )
    .withColumn(
        "order_date",
        F.to_timestamp("order_date")
    )
    .withColumn(
        "order_amount",
        F.col("order_amount").cast("double")
    )
    .withColumn(
        "quantity",
        F.col("quantity").cast("int")
    )
    .filter(F.col("order_amount") >= 0)
    .filter(F.col("quantity") > 0)
)

orders_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/orders")


transactions_clean = (
    transactions
    .dropDuplicates(["transaction_id"])
    .filter(
        F.col("transaction_id").isNotNull() &
        F.col("order_id").isNotNull()
    )
    .withColumn(
        "transaction_date",
        F.to_timestamp("transaction_date")
    )
    .withColumn(
        "amount",
        F.col("amount").cast("double")
    )
    .withColumn(
        "payment_status",
        F.lower(F.trim(F.col("payment_status")))
    )
    .filter(F.col("amount") >= 0)
)

transactions_clean.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{silver_path}/transactions")


# GOLD LAYER

customers = spark.read.format("delta").load(
    f"{silver_path}/customers"
)

products = spark.read.format("delta").load(
    f"{silver_path}/products"
)

orders = spark.read.format("delta").load(
    f"{silver_path}/orders"
)

transactions = spark.read.format("delta").load(
    f"{silver_path}/transactions"
)


sales_gold = (
    orders
    .join(
        products,
        orders.product_id == products.product_id,
        "left"
    )
    .join(
        customers,
        orders.customer_id == customers.customer_id,
        "left"
    )
    .select(
        orders.order_id,
        orders.customer_id,
        customers.customer_name,
        orders.product_id,
        products.product_name,
        products.category,
        orders.order_date,
        orders.quantity,
        orders.order_amount
    )
    .withColumn(
        "order_month",
        F.date_format("order_date", "yyyy-MM")
    )
)

sales_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/sales")


revenue_gold = (
    sales_gold
    .groupBy("order_month")
    .agg(
        F.sum("order_amount").alias("total_revenue"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn(
        "average_order_value",
        F.round(
            F.col("total_revenue") /
            F.col("total_orders"),
            2
        )
    )
)

revenue_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/revenue_metrics")


customer_ltv = (
    sales_gold
    .groupBy(
        "customer_id",
        "customer_name"
    )
    .agg(
        F.sum("order_amount").alias("lifetime_value"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_items"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date")
    )
    .withColumn(
        "average_order_value",
        F.round(
            F.col("lifetime_value") /
            F.col("total_orders"),
            2
        )
    )
)

customer_ltv.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/customer_ltv")


repeat_customers = (
    sales_gold
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.sum("order_amount").alias("total_spend")
    )
    .withColumn(
        "is_repeat_customer",
        F.when(
            F.col("order_count") > 1,
            True
        ).otherwise(False)
    )
)

repeat_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/repeat_customers")


product_performance = (
    sales_gold
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        F.sum("quantity").alias("units_sold"),
        F.sum("order_amount").alias("revenue"),
        F.countDistinct("order_id").alias("orders"),
        F.countDistinct("customer_id").alias("unique_customers")
    )
    .withColumn(
        "average_revenue_per_order",
        F.round(
            F.col("revenue") /
            F.col("orders"),
            2
        )
    )
    .orderBy(
        F.desc("revenue")
    )
)

product_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/product_performance")


customer_analysis = (
    sales_gold
    .groupBy(
        "customer_id",
        "customer_name"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("order_amount").alias("total_spend"),
        F.sum("quantity").alias("total_items"),
        F.avg("order_amount").alias("average_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date")
    )
    .withColumn(
        "customer_segment",
        F.when(
            F.col("total_spend") >= 10000,
            "High Value"
        )
        .when(
            F.col("total_spend") >= 5000,
            "Medium Value"
        )
        .otherwise("Low Value")
    )
)

customer_analysis.write \
    .format("delta") \
    .mode("overwrite") \
    .save(f"{gold_path}/customer_analysis")


repeat_purchase_rate = (
    repeat_customers
    .agg(
        F.round(
            F.sum(
                F.when(
                    F.col("is_repeat_customer"),
                    1
                ).otherwise(0)
            )
            /
            F.count("*") * 100,
            2
        ).alias("repeat_purchase_rate")
    )
)

repeat_purchase_rate.show()