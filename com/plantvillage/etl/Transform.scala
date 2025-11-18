package com.plantvillage.etl

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.functions._

object Transform {
  private val extractCrop = udf((folder: String) =>
    if (folder == null) "Unknown" else folder.split("[_\\s]+").headOption.getOrElse("Unknown").capitalize
  )

  private val extractDisease = udf((folder: String) =>
    if (folder == null) "unknown" else {
      val parts = folder.split("[_\\s]+")
      if (parts.length > 1) parts.drop(1).mkString("_").toLowerCase else "unknown"
    }
  )

  private val severityUdf = udf((d: String) => {
    if (d == null) "Unknown"
    else {
      val ds = d.toLowerCase
      if (ds.contains("healthy")) "None"
      else if (AppConfig.HIGH_SEVERITY.exists(ds.contains)) "High"
      else if (AppConfig.MEDIUM_SEVERITY.exists(ds.contains)) "Medium"
      else "Low"
    }
  })

  private val familyUdf = udf((c: String) => {
    val lc = if (c == null) "" else c.toLowerCase
    if (lc.contains("tomato") || lc.contains("potato") || lc.contains("pepper")) "Solanaceae"
    else if (lc.contains("apple")) "Rosaceae"
    else "Other"
  })

  // Accept java.lang.Double so we can check for null safely (Spark may pass null)
  private val sizeCategory = udf((kb: java.lang.Double) => {
    if (kb == null) "Unknown"
    else if (kb < 50.0) "Small"
    else if (kb < 100.0) "Medium"
    else "Large"
  })

  def run(df: DataFrame): DataFrame = {
    df.withColumn("crop_type", extractCrop(col("folder_name")))
      .withColumn("disease_raw", extractDisease(col("folder_name")))
      .withColumn("disease_class", lower(col("disease_raw")))
      .withColumn("is_healthy", lower(col("disease_raw")).contains("healthy"))
      .withColumn("severity_level", severityUdf(col("disease_class")))
      .withColumn("crop_family", familyUdf(col("crop_type")))
      .withColumn("size_category", sizeCategory(col("file_size_kb")))
      .withColumn("process_ts", current_timestamp())
      .select(
        "image_id", "path_str", "filename", "folder_name", "crop_type",
        "disease_class", "is_healthy", "severity_level", "crop_family",
        "size_category", "file_size_kb", "process_ts"
      )
  }
}
