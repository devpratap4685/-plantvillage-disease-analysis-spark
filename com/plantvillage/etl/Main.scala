package com.plantvillage.etl

import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark = SparkSession.builder()
    .appName(AppConfig.SPARK_APP_NAME)
    .master(AppConfig.SPARK_MASTER)
    .getOrCreate()

  import spark.implicits._

  try {
    // EXTRACT
    val raw = Extract.run(spark)
    Load.saveBronze(raw)

    // TRANSFORM
    val transformed = Transform.run(raw)
    Load.saveSilver(transformed)

    // STAR SCHEMA
    val dims = StarSchema.createDimensions(spark, transformed)
    val fact = StarSchema.createFact(transformed, dims)

    // SAVE DIMS & FACT
    Load.saveDim(dims("disease"), AppConfig.Tables.DIM_DISEASE)
    Load.saveDim(dims("crop"), AppConfig.Tables.DIM_CROP)
    Load.saveDim(dims("size"), AppConfig.Tables.DIM_SIZE)
    Load.saveFact(fact)

    // ANALYTICS: compute and persist insights
    Analytics.computeAndSave(spark, transformed)

    println(s"Completed. Bronze=${raw.count()} Silver=${transformed.count()} Fact=${fact.count()}")

  } finally {
    spark.stop()
  }
}