package com.landy.spark.hudi.quitstart

import org.apache.hudi.DataSourceReadOptions._
import org.apache.spark.sql.SparkSession

/**
 *
 */
object QuitStartQueryPointInTimeDemo{

    def main(args: Array[String]): Unit = {
        val tableName = "hudi_trips_cow"
        val basePath =  Constant.HUDI_STORAGE_PATH+tableName

        val spark = SparkSession.builder.appName("test-hudi-query-point-intime").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local")
                .getOrCreate()


        spark.read.format("org.apache.hudi").load(basePath + "/*/*/*/*")
                .createOrReplaceTempView("hudi_trips_snapshot")
        import spark.implicits._
        val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from hudi_trips_snapshot order by commitTime").map(k => k.getString(0)).take(50)

        val beginTime = "000" // Represents all commits > this time.
        val endTime = commits(commits.length - 2) // commit time we are interested in

        //incrementally query data
        val tripsPointInTimeDF = spark.read.format("org.apache.hudi").
                option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
                option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
                option(END_INSTANTTIME_OPT_KEY, endTime).
                load(basePath)
        tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
        spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_point_in_time where fare > 20.0").show()
        spark.close()

    }

}
