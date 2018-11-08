package com.htg.sparkml.trainning.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReadDataFromCSV {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("Demo").master("local[2]").getOrCreate();

        Dataset<Row> people = spark.read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .schema(new StructType(new StructField[]{DataTypes.createStructField("name", DataTypes.StringType,true),
                        DataTypes.createStructField("age",DataTypes.IntegerType,true)}))
                .load("/Users/xingshulin/IdeaProjects/sparkMLtranning/datasources/people.csv");

        people.show();
        people.select(people.col("name")).write().mode(SaveMode.Overwrite).format("parquet").save("/Users/xingshulin/IdeaProjects/sparkMLtranning/datasources/people_names.parquet");
        System.out.println("filter name success.");

        System.out.println("read again from parquet...");
        Dataset<Row> peopleNamesFromParquet = spark.read().format("parquet").load("/Users/xingshulin/IdeaProjects/sparkMLtranning/datasources/people_names.parquet");
        peopleNamesFromParquet.show();

    }
}
