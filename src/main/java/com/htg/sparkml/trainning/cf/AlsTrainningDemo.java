package com.htg.sparkml.trainning.cf;

import org.apache.commons.lang.time.StopWatch;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Instant;

/**
 * 使用历史数据，对CF模型超参调整
 *
 * @author huangtiangang
 */
public class AlsTrainningDemo {

    public static void main(String[] args) {

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        SparkSession sparkSession = SparkSession.builder()
//                .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 0)
                .config("spark.sql.execution.useObjectHashAggregateExec", false)
                .config("spark.ui.enabled", false)
                .appName("epocket CF demo").master("local[2]").getOrCreate();

        sparkSession.sparkContext().setLogLevel("warn");

        // _uid _type _nid _views  id  user_id  nid
        Dataset<Row> mainContent = EpocketDataSource.getMainContent(sparkSession);
        mainContent.show(20);

        Dataset<Row> rowDataset = mainContent.select("user_id", "nid", "_views");
        rowDataset = rowDataset.withColumn("_views", rowDataset.col("_views").cast(DataTypes.LongType));

        Dataset<Row>[] datasets = rowDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainning = datasets[0];
        Dataset<Row> test = datasets[1];
        test.show(20);
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("user_id")
                .setItemCol("nid")
                .setRatingCol("_views");

        ALSModel model = als.fit(trainning);
        model.setColdStartStrategy("drop");

        mainContent.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/Users/xingshulin/IdeaProjects/sparkMLtranning/spark-warehouse/main_content.parquet");

        Dataset<Row> rowDataset1 = model.recommendForAllUsers(100);

        rowDataset1.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/Users/xingshulin/IdeaProjects/sparkMLtranning/spark-warehouse/recommendations.parquet");
        sparkSession.stop();

        stopWatch.stop();
        java.util.Date consumeTime = Date.from(Instant.ofEpochMilli(stopWatch.getTime()));
        System.out.println("花费时间:" + new SimpleDateFormat("dd-mm-ss").format(consumeTime));

    }
}
