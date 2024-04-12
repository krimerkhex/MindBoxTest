from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

test_data = {
    "Products": [{"id": 1, "product": "Bread"},
                 {"id": 2, "product": "Load"},
                 {"id": 3, "product": "Creckers"},
                 {"id": 4, "product": "Baguette"},
                 {"id": 5, "product": "Pou"},
                 {"id": 6, "product": "Bull"},
                 {"id": 7, "product": "Cow"},
                 {"id": 8, "product": "Chicken"},
                 {"id": 9, "product": "Pig"},
                 {"id": 10, "product": "Eggs"},
                 {"id": 11, "product": "Milk"},
                 {"id": 12, "product": "Chocolate"},
                 {"id": 13, "product": "Hazelnut"}
                 ],

    "Categories": [{"id": 1, "category": "Meet"},
                   {"id": 2, "category": "Flour"},
                   {"id": 3, "category": "Souse"},
                   {"id": 4, "category": "Tasty"},
                   {"id": 5, "category": "Coffee"}],

    "Couples": [{"product_id": 2, "category_id": 1},
                {"product_id": 2, "category_id": 2},
                {"product_id": 2, "category_id": 3},
                {"product_id": 2, "category_id": 4},
                {"product_id": 1, "category_id": 6},
                {"product_id": 1, "category_id": 7},
                {"product_id": 1, "category_id": 8},
                {"product_id": 1, "category_id": 9},
                {"product_id": 4, "category_id": 1},
                {"product_id": 4, "category_id": 2},
                {"product_id": 4, "category_id": 3},
                {"product_id": 4, "category_id": 4},
                {"product_id": 4, "category_id": 6},
                {"product_id": 4, "category_id": 7},
                {"product_id": 4, "category_id": 8},
                {"product_id": 4, "category_id": 9}, ]
}


def get_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("") \
            .master("local[*]") \
            .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0') \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    except Exception as ex:
        pass
    return spark


def get_pairs(session: SparkSession):
    print(session.sparkContext)
    df_products = session.createDataFrame(test_data["Products"])
    df_categories = session.createDataFrame(test_data["Categories"])
    df_couples = session.createDataFrame(test_data["Couples"])
    # df_products.show()
    # df_categories.show()
    # df_couples.show()

    products_without_category = df_products.join(df_couples, on=df_products["id"] == df_couples["product_id"],
                                                 how='left') \
        .filter(col("category_id").isNull()) \
        .select("product") \
        .withColumn("category", lit("Without category"))

    pairs_df = df_couples.join(df_products, df_couples["product_id"] == df_products["id"]) \
        .join(df_categories, df_couples["category_id"] == df_categories["id"]) \
        .select(df_products["product"], df_categories["category"])

    result_df = pairs_df.union(products_without_category)

    result_df.show()


if __name__ == '__main__':
    get_pairs(get_spark_session())
