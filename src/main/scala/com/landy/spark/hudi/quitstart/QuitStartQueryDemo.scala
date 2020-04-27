package com.landy.spark.hudi.quitstart

import org.apache.spark.sql.SparkSession

/**
 *
 */
object QuitStartQueryDemo{

    def main(args: Array[String]): Unit = {
        val tableName = "hudi_trips_cow"
        val basePath =  Constant.HUDI_STORAGE_PATH+tableName

        val spark = SparkSession.builder.appName("test-hudi-query").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local")
                .getOrCreate()

        val tripsSnapshotDF = spark.read.format("org.apache.hudi").
                load(basePath + "/*/*/*/*")
        tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")
        spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
        spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

        spark.close()

    }

}
