import argparse
import pandas as pd
from pyspark.sql import SparkSession


def create_spark_views(spark: SparkSession, supplier_car_data: str):
    spark.read.json(supplier_car_data).createOrReplaceTempView("supplier_car")

def return_ten_columns(spark: SparkSession):
    result = spark.sql("""SELECT * FROM supplier_car limit 10""").collect()
    return result

def run_transformations(spark: SparkSession, supplier_car_data: str, output_location: str):
    create_spark_views(spark, supplier_car_data)
    transformed_view = return_ten_columns(spark)
    supplier_df = spark.createDataFrame(data=transformed_view)
    pandas_supplier_df = supplier_df.toPandas()
    # create excel writer
    with  pd.ExcelWriter('target_data.xlsx') as writer:
        # write dataframe to excel sheet named 'marks'
        pandas_supplier_df.to_excel(writer)
        # save the excel file
        writer.save()
    print('DataFrame is written successfully to Excel Sheet.')

def to_canonical_date_str(date_to_transform):
    return date_to_transform.strftime('%Y-%m-%d')


if __name__ == "__main__":
    spark_session = (
            SparkSession.builder
                        .master("local[2]")
                        .appName("DataTest")
                        .config("spark.executorEnv.PYTHONHASHSEED", "0")
                        .getOrCreate()
    )

    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--supplier_car', required=False, default="./src/input_data/supplier_car.json")
    parser.add_argument('--output_location', required=False, default="./src/output_data/target_data.csv")
    args = vars(parser.parse_args())

    run_transformations(spark_session, args['supplier_car'], args['output_location'])