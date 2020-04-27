package com.landy.spark.hudi.quitstart


import org.apache.spark.sql.SaveMode.Append
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SparkSession

/**
 *
 * @date 2020/4/25  22:19
 * @author liangriyu
 */
object QuitStartUpdateDemo {

    def main(args: Array[String]): Unit = {
        val tableName = "hudi_trips_cow"
        val basePath =  Constant.HUDI_STORAGE_PATH+tableName
        val dataGen = new DataGenerator
        val spark = SparkSession.builder.appName("test-hudi-update").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local")
                .getOrCreate()
        val updates = convertToStringList(dataGen.generateUpdates(10))
        val df = spark.read.json(spark.sparkContext.parallelize(updates, 2))
        df.write.format("org.apache.hudi").
                options(getQuickstartWriteConfigs).
                option(PRECOMBINE_FIELD_OPT_KEY, "ts").
                option(RECORDKEY_FIELD_OPT_KEY, "uuid").
                option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
                option(TABLE_NAME, tableName).
                mode(Append).
                save(basePath)
        spark.close()

    }

}
