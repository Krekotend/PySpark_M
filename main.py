from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("Products") \
    .getOrCreate()

# Продукты
products_data = [("P1", "Product 1"),
                 ("P2", "Product 2"),
                 ("P3", "Product 3"),
                 ("P4", "Product 4"),
                 ("P5", "Product 5")]

# Категории
categories_data = [("C1", "Category 1"),
                   ("C2", "Category 2"),
                   ("C3", "Category 3"),
                   ("C4", "Category 4"),
                   ("C5", "Category 5")]

# Связи между продуктами и категориями
product_category_data = [("P1", "C1"),
                         ("P1", "C2"),
                         ("P2", "C2"),
                         ("P3", "C3"),
                         ("P4", None),
                         ("P5", "C1"),
                         ("P5", "C2")]

products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

result_df = product_category_df \
    .join(products_df, on="product_id") \
    .join(categories_df, on="category_id", how="left") \
    .select("product_name", "category_name")

# Показать результат обединеия продуктов с категориями
result_df.show()

no_category_products_df = product_category_df \
    .filter(col("category_id").isNull()) \
    .join(products_df, on="product_id") \
    .select("product_name")

# Показать продукты, у которых нет категорий
no_category_products_df.show()

# Так же возможно вывести все категории без продуктов
category_without_product_df = categories_df \
    .join(product_category_df, on="category_id", how="left") \
    .filter(col("product_id").isNull()) \
    .select("category_name")

# Показать все категории, к которым не привязан ни один продукт
category_without_product_df.show()
