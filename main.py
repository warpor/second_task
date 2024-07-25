from typing import Tuple

from pyspark.sql import DataFrame, SparkSession

from utils import get_df_with_products_and_categories


def _create_frames(spark_session: SparkSession) -> Tuple[DataFrame, DataFrame, DataFrame]:
    products: DataFrame = spark_session.createDataFrame([
        {"id": 1, "product": "milk"},
        {"id": 2, "product": "meat"},
        {"id": 3, "product": "bread"}
    ])

    categories: DataFrame = spark_session.createDataFrame([
        {"id": 1, "category": "eat"},
        {"id": 2, "category": "drink"},
        {"id": 3, "category": "health"}
    ])

    connection_df: DataFrame = spark_session.createDataFrame([
        {"product_id": 1, "category_id": 1},
        {"product_id": 1, "category_id": 2},
        {"product_id": 1, "category_id": 3},
        {"product_id": 2, "category_id": 1},
        {"product_id": 2, "category_id": 3}
    ])

    return products, categories, connection_df


def create_spark_session() -> SparkSession:
    spark_session = SparkSession.builder \
        .appName("ProductCategoryTest") \
        .master("local[*]") \
        .getOrCreate()

    return spark_session


def show_df(spark_session: SparkSession):
    products, categories, connection_df = _create_frames(spark_session)

    print("Продукты:")
    products.show()

    print("Категории:")
    categories.show()

    print("Результат:")
    get_df_with_products_and_categories(products, categories, connection_df).show()
