from pyspark.sql import DataFrame


def get_df_with_products_and_categories(products: DataFrame, categories: DataFrame,
                                        connection_df: DataFrame) -> DataFrame:
    first_join = products.join(connection_df, products.id == connection_df.product_id,
                               how="left")
    second_join = first_join.join(categories, first_join.category_id == categories.id,
                                  how="left")
    return second_join.select(second_join.product, second_join.category)



