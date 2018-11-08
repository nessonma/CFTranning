package com.htg.sparkml.trainning.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class DataSourceUserLocalHive {

    public static void main(String[] args) {
//        createTable();
        getTable();
    }

    private static void createTable() {
                SparkSession spark = SparkSession.builder().appName("Demo").master("local[2]").getOrCreate();

        Dataset<Row> people = spark.read()
                .format("csv")
                .option("inferSchema", true)
                .option("header", true)
                .schema(new StructType(new StructField[]{DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField("age", DataTypes.IntegerType, true)}))
                .load("/Users/xingshulin/IdeaProjects/sparkMLtranning/datasources/people.csv");

        // 创建数据库
        people.write()
                .bucketBy(20, "age")
                .mode(SaveMode.ErrorIfExists)
                .saveAsTable("people");

        spark.close();
    }

    private static void getTable() {
        //读取已经持久化的表
        SparkSession spark1 = SparkSession.builder().appName("Demo")
                .master("local[1]").getOrCreate();
        Dataset<Row> people1 = spark1.read().table("people");
        people1.show();
        spark1.close();
    }

}
