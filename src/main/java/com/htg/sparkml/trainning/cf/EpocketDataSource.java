package com.htg.sparkml.trainning.cf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class EpocketDataSource {

    private static final String PATH = EpocketDataSource.class.getClassLoader().getResource("").getPath()
            + "datasources/epocket/";

    private static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("_uid", StringType, false),
            DataTypes.createStructField("_type", StringType, false),
            DataTypes.createStructField("_nid", StringType, false),
            DataTypes.createStructField("_views", StringType, false),
    });

    static Dataset<Row> getMainContent(SparkSession sparkSession) {
        Dataset<Row> mainContents = getDataFromCsv(sparkSession, PATH + "page_view_1_2018-09-02_2018-09-30.csv");
        Dataset<Row> quanContents = getDataFromCsv(sparkSession, PATH + "page_view_2_2018-09-02_2018-09-30.csv");
        Dataset<Row> union = mainContents.union(quanContents);
        Dataset<Row> rowDataset = attachIdCol(sparkSession, union);
        Dataset<Row> rowDataset1 = attachUidCol(sparkSession, rowDataset);
        Dataset<Row> rowDataset2 = attachNidCol(sparkSession, rowDataset1);
        return rowDataset2;
    }

    private static Dataset<Row> getDataFromCsv(SparkSession sparkSession, String cvsFile) {
        return sparkSession.read()
                .format("csv")
                .option("header", true)
                .schema(SCHEMA)
                .load(cvsFile);
    }


    private static Dataset<Row> attachIdCol(SparkSession sparkSession, Dataset<Row> contents) {
        JavaPairRDD<Row, Long> rowLongJavaPairRDD = contents.repartition(10).javaRDD().zipWithUniqueId();
        JavaRDD<Row> rowJavaRDD = rowLongJavaPairRDD.flatMap(EpocketDataSource::appendUniqueId);

        StructType structType = contents.schema().add(DataTypes.createStructField("id", LongType, false));
        return sparkSession.createDataFrame(rowJavaRDD, structType);
    }

    private static Dataset<Row> attachUidCol(SparkSession sparkSession, Dataset<Row> contents) {
        Column uid = contents.col("_uid");
        contents = contents.repartition(uid.startsWith("u"), uid.startsWith("d"));

        JavaRDD<Row> rows = contents.javaRDD().zipWithUniqueId().flatMap(EpocketDataSource::appendUniqueId);

        return sparkSession.createDataFrame(rows, contents.schema().add(createStructField("user_id", LongType, false)));
    }

    private static Dataset<Row> attachNidCol(SparkSession sparkSession, Dataset<Row> contents) {
        Column type = contents.col("_type");
        JavaPairRDD<Row, Long> rowLongJavaPairRDD = contents.repartition(type).javaRDD().zipWithUniqueId();
        JavaRDD<Row> rowJavaRDD = rowLongJavaPairRDD.flatMap(EpocketDataSource::appendUniqueId);
        Dataset<Row> nid = sparkSession.createDataFrame(rowJavaRDD, contents.schema().add("nid", LongType, false));
        return nid;
    }

    private static Iterator<Row> appendUniqueId(Tuple2<Row, Long> rowLongTuple2) {
        Row row = rowLongTuple2._1();
        Long userId = rowLongTuple2._2();
        List fields = new ArrayList();
        for (int i = 0; i < row.size(); i++) {
            fields.add(row.get(i));
        }
        fields.add(userId);
        Row row1 = RowFactory.create(fields.toArray());
        return Collections.singleton(row1).iterator();
    }
}
