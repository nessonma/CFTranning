package com.htg.sparkml.trainning.cf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AlsTrainningOutDemo {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 1024 * 1024)
                .appName("epocket CF demo").master("local[2]").getOrCreate();

        Dataset<Row> mainContent = sparkSession.read()
                .parquet("/Users/xingshulin/IdeaProjects/sparkMLtranning/spark-warehouse/main_content.parquet");

        Dataset<Row> recommends = sparkSession.read()
                .parquet("/Users/xingshulin/IdeaProjects/sparkMLtranning/spark-warehouse/recommendations.parquet");

        Dataset<Row> join = mainContent.join(recommends,
                mainContent.col("user_id")
                        .equalTo(recommends.col("user_id")))
                .select("_uid", "_type", "_nid", "recommendations");

        join.show(10);
        join.printSchema();
        
        sparkSession.stop();
    }
}
