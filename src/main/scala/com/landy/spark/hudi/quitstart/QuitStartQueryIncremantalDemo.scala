package com.landy.spark.hudi.quitstart

import org.apache.hudi.DataSourceReadOptions.{BEGIN_INSTANTTIME_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_OPT_KEY}
import org.apache.spark.sql.SparkSession

/**
 *
 */
object QuitStartQueryIncremantalDemo{

    def main(args: Array[String]): Unit = {
        val tableName = "hudi_trips_cow"
        val basePath =  Constant.HUDI_STORAGE_PATH+tableName

        val spark = SparkSession.builder.appName("test-hudi-query-incremental").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local")
                .getOrCreate()


        spark.read.format("org.apache.hudi").load(basePath + "/*/*/*/*")
                .createOrReplaceTempView("hudi_trips_snapshot")
        import spark.implicits._
        val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime")
                .map(k => k.getString(0)).take(50)
        val beginTime = commits(commits.length - 2) // commit time we are interested in

        // incrementally query data
        val tripsIncrementalDF = spark.read.format("org.apache.hudi").
                option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
                option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
                load(basePath)
        tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

        spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()


        spark.close()

    }

}
