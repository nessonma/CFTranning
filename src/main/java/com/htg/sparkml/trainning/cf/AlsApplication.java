package com.htg.sparkml.trainning.cf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 使用历史数据，对CF模型超参调整
 * @author huangtiangang
 */
public class AlsApplication {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("epocket CF demo").master("local[2]").getOrCreate();

        // id uid type nid views
        Dataset<Row> mainContent = EpocketDataSource.getMainContent(sparkSession);

        mainContent.show(20);
        sparkSession.stop();
    }
}
