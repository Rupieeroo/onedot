import argparse
import pandas as pd
from pyspark.sql import SparkSession


def create_spark_views(spark: SparkSession, supplier_car_data: str):
    spark.read.json(supplier_car_data).createOrReplaceTempView("supplier_car")

def collect_table(spark: SparkSession):
    result = spark.sql(
        """
            SELECT 
                ''          AS carType,
                ''          AS color,
                ''          AS condition,
                ''          AS currency,
                ''          AS drive,
                ''          AS city,
                ''          AS country,
                MakeText    AS make,
                ''          AS manufacture_year,
                ''          AS milage,
                ''          AS milage_unit,
                ModelText   AS model,
                TypeName    AS model_variant,
                ''          AS price_on_request,
                'car'       AS type,
                ''          AS zip,
                ''          AS manufacture_month,
                ''          AS fuel_consumption_unit
            FROM 
                supplier_car
        """
    ).collect()
    return result

def pre_processing(spark: SparkSession, supplier_car_data: str, output_location: str):
    # runs the data transformations
    create_spark_views(spark, supplier_car_data)
    transformed_view = collect_table(spark)
    supplier_df = spark.createDataFrame(data=transformed_view)
    pandas_supplier_df = supplier_df.toPandas()
    # creates excel writer
    with  pd.ExcelWriter(output_location) as writer:
        # writes dataframe to excel sheet
        pandas_supplier_df.to_excel(writer)
        # save the excel file
        writer.save()
    print('The DataFrame has been successfully written to an Excel Sheet.')


if __name__ == "__main__":
    spark_session = (
            SparkSession.builder
                        .master("local[2]")
                        .appName("DataTest")
                        .config("spark.executorEnv.PYTHONHASHSEED", "0")
                        .getOrCreate()
    )

    parser = argparse.ArgumentParser(description='SupplierCarLoad')
    parser.add_argument('--supplier_car', required=False, default="./src/input_data/supplier_car.json")
    parser.add_argument('--output_location', required=False, default="./src/output_data/target_data.xlsx")
    args = vars(parser.parse_args())

    pre_processing(spark_session, args['supplier_car'], args['output_location'])