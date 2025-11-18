package com.plantvillage.etl

import java.util.Properties

object AppConfig {
  private val props = new Properties()
  props.load(getClass.getResourceAsStream("/config.properties"))

  val APP_NAME: String = props.getProperty("app.name")
  val APP_VERSION: String = props.getProperty("app.version")

  val SPARK_MASTER: String = props.getProperty("spark.master")
  val SPARK_APP_NAME: String = props.getProperty("spark.app.name")

  val INPUT_PATH: String = props.getProperty("paths.input")

  val DB_URL: String = props.getProperty("db.url")
  val DB_USER: String = props.getProperty("db.user")
  val DB_PASS: String = props.getProperty("db.password")
  val DB_DRIVER: String = props.getProperty("db.driver")
  val DB_SCHEMA: String = props.getProperty("db.schema")

  object Tables {
    val BRONZE = props.getProperty("db.table.bronze")
    val SILVER = props.getProperty("db.table.silver")
    val FACT = props.getProperty("db.table.fact")
    val DIM_DISEASE = props.getProperty("db.table.dim_disease")
    val DIM_CROP = props.getProperty("db.table.dim_crop")
    val DIM_SIZE = props.getProperty("db.table.dim_size")

    val ANALYTICS_SUMMARY = props.getProperty("db.table.analytics_summary")
    val DISEASE_STATS = props.getProperty("db.table.disease_stats")
    val CROP_STATS = props.getProperty("db.table.crop_stats")
    val SEVERITY_STATS = props.getProperty("db.table.severity_stats")
    val FILE_SIZE_STATS = props.getProperty("db.table.file_size_stats")
  }

  private def csvToArray(s: String): Array[String] =
    if (s == null || s.trim.isEmpty) Array.empty else s.split(",").map(_.trim.toLowerCase)

  val HIGH_SEVERITY: Array[String] = csvToArray(props.getProperty("business.severity.high"))
  val MEDIUM_SEVERITY: Array[String] = csvToArray(props.getProperty("business.severity.medium"))

  val TOP_N: Int = props.getProperty("analytics.topn", "10").toInt
  val SAMPLE: Int = props.getProperty("analytics.sample", "5").toInt

  def jdbcUrlWithSchema: String = s"$DB_URL?currentSchema=$DB_SCHEMA"
}
