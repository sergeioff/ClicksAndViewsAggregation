package com.pogorelovs.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ClicksAndViewsAggregation {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .getOrCreate()

    val viewsSchema = StructType(Seq(
      StructField("timestamp", LongType),
      StructField("email", StringType),
      StructField("ip", StringType)
    ))

    val clicksSchema = StructType(Seq(
      StructField("timestamp", LongType),
      StructField("element", StringType),
      StructField("email", StringType)
    ))

    val clicks = sparkSession.read.schema(clicksSchema)
      .option("header", value = true)
      .csv("data/clicks/data.csv")
      .withColumn("action", concat_ws("-", lit("click"), col("element")))
      .drop(col("element"))

    val views = sparkSession.read.schema(viewsSchema)
      .option("header", value = true)
      .csv("data/views/data.csv")
      .withColumn("action", lit("view"))

    val enrichedClicks = clicks.join(views.select(col("email"), col("ip")), "email")

    val union = enrichedClicks
      .select(col("email"), col("timestamp"), col("action"), col("ip"))
      .union(views.select(
        col("email"), col("timestamp"), col("action"), col("ip")
      ))

    val unionWithMaskedIp = union.withColumn("masked_ip", regexp_replace(col("ip"), "\\d+$", "0"))

    val salt = "salt"
    val unionWithMaskedEmailsAndIp = unionWithMaskedIp.withColumn("hashed_email", upper(md5(concat(col("email"), lit(salt)))))

    unionWithMaskedEmailsAndIp
      .drop("ip", "email")
      .sort(col("timestamp"))
      .select(col("timestamp"), col("action"), col("hashed_email"), col("masked_ip"))
      .repartition(partitionExprs = col("timestamp").divide(200).cast(IntegerType))
      .coalesce(2) // since we want to have exactly two files
      .write
      .option("header", value = true)
      .csv("backup")
  }
}
