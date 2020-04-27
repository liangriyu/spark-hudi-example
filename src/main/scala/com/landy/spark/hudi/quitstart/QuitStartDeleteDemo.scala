package com.landy.spark.hudi.quitstart

import org.apache.spark.sql.SaveMode._
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
object QuitStartDeleteDemo {

    def main(args: Array[String]): Unit = {
        val tableName = "hudi_trips_cow"
        val basePath =  Constant.HUDI_STORAGE_PATH+tableName
        val dataGen = new DataGenerator
        val spark = SparkSession.builder.appName("test-hudi-delete").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .master("local")
                .getOrCreate()
        // fetch total records count
        spark.sql("select uuid, partitionPath from hudi_trips_snapshot").count()
        // fetch two records to be deleted
        val ds = spark.sql("select uuid, partitionPath from hudi_trips_snapshot").limit(2)

        // issue deletes
        val deletes = dataGen.generateDeletes(ds.collectAsList())
        val df = spark.read.json(spark.sparkContext.parallelize(deletes, 2));
        df.write.format("org.apache.hudi").
                options(getQuickstartWriteConfigs).
                option(OPERATION_OPT_KEY,"delete").
                option(PRECOMBINE_FIELD_OPT_KEY, "ts").
                option(RECORDKEY_FIELD_OPT_KEY, "uuid").
                option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
                option(TABLE_NAME, tableName).
                mode(Append).
                save(basePath)

        // run the same read query as above.
        val roAfterDeleteViewDF = spark.
                read.
                format("org.apache.hudi").
                load(basePath + "/*/*/*/*")
        roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
        // fetch should return (total - 2) records
        spark.sql("select uuid, partitionPath from hudi_trips_snapshot").count()

        spark.close()

    }

}
