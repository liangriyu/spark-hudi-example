package com.landy.spark.hudi.quitstart

import org.apache.spark.sql.SaveMode._
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SparkSession

/**
 *
 */
object QuitStartInsertDemo{

    def main(args: Array[String]): Unit = {
        val tableName = "hudi_trips_cow"
        val basePath =  Constant.HUDI_STORAGE_PATH+tableName
        val dataGen = new DataGenerator
        val list= dataGen.generateInserts(10);
        val inserts = convertToStringList(list)
        val spark = SparkSession.builder.appName("test-hudi-insert").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local")
                .getOrCreate()

        val df = spark.read.json(spark.sparkContext.parallelize(inserts, 2))
        df.write.format("org.apache.hudi").
                options(getQuickstartWriteConfigs).
                option(PRECOMBINE_FIELD_OPT_KEY, "ts").
                option(RECORDKEY_FIELD_OPT_KEY, "uuid").
                option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
                option(TABLE_NAME, tableName).
                mode(Overwrite).
                save(basePath)

        spark.close()

    }

}
