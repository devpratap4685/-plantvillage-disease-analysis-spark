package com.plantvillage.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Analytics {

  def computeAndSave(spark: SparkSession, transformed: DataFrame): Unit = {
    import spark.implicits._

    // summary
    val total = transformed.count()
    val healthy = transformed.filter($"is_healthy").count()
    val diseased = total - healthy
    val summary = Seq(
      ("total_images", total),
      ("healthy_count", healthy),
      ("diseased_count", diseased)
    ).toDF("metric", "value")

    // disease stats
    val diseaseStats = transformed.filter(!$"is_healthy")
      .groupBy("disease_class", "crop_type", "severity_level")
      .count()
      .orderBy(desc("count"))

    // crop stats
    val cropStats = transformed.groupBy("crop_type")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))

    // severity distribution
    val severityStats = transformed.groupBy("severity_level")
      .agg(count("*").alias("count"))
      .orderBy(desc("count"))

    // file size aggregates
    val fileSizeStats = transformed.groupBy("size_category")
      .agg(
        count("*").alias("image_count"),
        round(avg("file_size_kb"), 2).alias("avg_size_kb"),
        round(min("file_size_kb"), 2).alias("min_size_kb"),
        round(max("file_size_kb"), 2).alias("max_size_kb")
      )

    // save to Postgres
    Load.save(summary, AppConfig.Tables.ANALYTICS_SUMMARY)
    Load.save(diseaseStats, AppConfig.Tables.DISEASE_STATS)
    Load.save(cropStats, AppConfig.Tables.CROP_STATS)
    Load.save(severityStats, AppConfig.Tables.SEVERITY_STATS)
    Load.save(fileSizeStats, AppConfig.Tables.FILE_SIZE_STATS)
  }
}
