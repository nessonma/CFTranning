package com.htg.sparkml.trainning.cf;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

/**
 * 使用历史数据，对CF模型超参调整
 *
 * @author huangtiangang
 */
public class AlsApplication {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .config("spark.sql.objectHashAggregate.sortBased.fallbackThreshold", 1024 * 1024)
                .appName("epocket CF demo").master("local[2]").getOrCreate();

        // _uid _type _nid _views  id  user_id  nid
        Dataset<Row> mainContent = EpocketDataSource.getMainContent(sparkSession);
        mainContent.show(20);

        Dataset<Row> rowDataset = mainContent.select("user_id", "nid", "_views");
        rowDataset = rowDataset.withColumn("_views", rowDataset.col("_views").cast(DataTypes.LongType));

        Dataset<Row>[] datasets = rowDataset.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> tranning = datasets[0];

        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("user_id")
                .setItemCol("nid")
                .setRatingCol("_views");

        ALSModel model = als.fit(tranning);
        model.setColdStartStrategy("drop");

        Dataset<Row> rowDataset1 = model.recommendForAllUsers(1);

        mainContent.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/Users/xingshulin/IdeaProjects/sparkMLtranning/spark-warehouse/main_content.parquet");

        rowDataset1.write()
                .format("parquet")
                .mode(SaveMode.Overwrite)
                .save("/Users/xingshulin/IdeaProjects/sparkMLtranning/spark-warehouse/recommendations.parquet");

        sparkSession.stop();
    }
}
