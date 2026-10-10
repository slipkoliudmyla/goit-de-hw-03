from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as spark_round, sum as spark_sum

# Створюємо сесію Spark
spark = SparkSession.builder.appName("goit-de-hw-03").getOrCreate()

# 1. Завантажуємо та читаємо кожен CSV-файл як окремий DataFrame
users_df = spark.read.csv("./users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv("./purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv("./products.csv", header=True, inferSchema=True)

print("1. Початкові дані")
users_df.show(5)
purchases_df.show(5)
products_df.show(5)

# 2. Очищуємо дані: видаляємо рядки з пропущеними значеннями
users_clean = users_df.dropna()
purchases_clean = purchases_df.dropna()
products_clean = products_df.dropna()

print("2. Кількість рядків до / після очищення")
print("users:", users_df.count(), "->", users_clean.count())
print("purchases:", purchases_df.count(), "->", purchases_clean.count())
print("products:", products_df.count(), "->", products_clean.count())
users_clean.show(5)
purchases_clean.show(5)
products_clean.show(5)

# Об'єднуємо покупки з продуктами і рахуємо суму кожної покупки
purchases_products = purchases_clean \
    .join(products_clean, on="product_id", how="inner") \
    .withColumn("total_price", col("quantity") * col("price"))

# 3. Загальна сума покупок за кожною категорією продуктів
total_by_category = purchases_products \
    .groupBy("category") \
    .agg(spark_round(spark_sum("total_price"), 2).alias("total_purchases")) \
    .orderBy(col("total_purchases").desc())

print("3. Загальна сума покупок за категоріями")
total_by_category.show()

# Додаємо дані користувачів і залишаємо лише вік 18-25 включно
purchases_18_25 = purchases_products \
    .join(users_clean, on="user_id", how="inner") \
    .where((col("age") >= 18) & (col("age") <= 25))

# 4. Сума покупок за кожною категорією для віку 18-25
total_by_category_18_25 = purchases_18_25 \
    .groupBy("category") \
    .agg(spark_round(spark_sum("total_price"), 2).alias("total_purchases_18_25")) \
    .orderBy(col("total_purchases_18_25").desc())

print("4. Сума покупок за категоріями (вік 18-25)")
total_by_category_18_25.show()

# 5. Частка покупок кожної категорії від сумарних витрат (вік 18-25)
total_spent_18_25 = purchases_18_25.agg(spark_sum("total_price")).collect()[0][0]

share_by_category_18_25 = total_by_category_18_25 \
    .withColumn(
        "percentage",
        spark_round(col("total_purchases_18_25") / total_spent_18_25 * 100, 2),
    ) \
    .orderBy(col("percentage").desc())

print("5. Частка покупок за категоріями (вік 18-25), %")
print("Сумарні витрати 18-25:", round(total_spent_18_25, 2))
share_by_category_18_25.show()

# 6. Топ-3 категорії з найвищим відсотком витрат (вік 18-25)
top_3_categories = share_by_category_18_25 \
    .select("category", "percentage") \
    .orderBy(col("percentage").desc()) \
    .limit(3)

print("6. Топ-3 категорії (вік 18-25)")
top_3_categories.show()

# Закриваємо сесію Spark
spark.stop()
