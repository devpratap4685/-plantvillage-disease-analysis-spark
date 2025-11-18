package com.plantvillage.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object StarSchema {

  def createDimensions(spark: SparkSession, df: DataFrame): Map[String, DataFrame] = {
    import spark.implicits._

    val dimDisease = df.select("disease_class", "is_healthy", "severity_level")
      .distinct()
      .withColumn("disease_key", monotonically_increasing_id().cast("int"))
      .withColumn("disease_id", concat(lit("DIS_"), col("disease_key")))
      .select("disease_key", "disease_id", "disease_class", "is_healthy", "severity_level")

    val dimCrop = df.select("crop_type", "crop_family")
      .distinct()
      .withColumn("crop_key", monotonically_increasing_id().cast("int"))
      .withColumn("crop_id", concat(lit("CRP_"), col("crop_key")))
      .select("crop_key", "crop_id", "crop_type", "crop_family")

    val dimSize = df.select("size_category")
      .distinct()
      .withColumn("size_key", monotonically_increasing_id().cast("int"))
      .withColumn("size_id", concat(lit("SIZ_"), col("size_key")))
      .select("size_key", "size_id", "size_category")

    Map("disease" -> dimDisease, "crop" -> dimCrop, "size" -> dimSize)
  }

  def createFact(df: DataFrame, dims: Map[String, DataFrame]): DataFrame = {
    df.join(dims("disease"), Seq("disease_class", "is_healthy", "severity_level"), "left")
      .join(dims("crop"), Seq("crop_type", "crop_family"), "left")
      .join(dims("size"), Seq("size_category"), "left")
      .select(
        col("image_id"),
        col("disease_key"),
        col("crop_key"),
        col("size_key"),
        col("file_size_kb"),
        col("process_ts").alias("processing_ts")
      )
  }
}
