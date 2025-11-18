
package com.plantvillage.etl

import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties

object Load {
  private def jdbcProps(): Properties = {
    val p = new Properties()
    p.setProperty("user", AppConfig.DB_USER)
    p.setProperty("password", AppConfig.DB_PASS)
    p.setProperty("driver", AppConfig.DB_DRIVER)
    p
  }

  // Ensure table name is schema-qualified: if caller passes "fact_image", we write to "gold.fact_image"
  private def qualify(table: String): String = {
    if (table.contains(".")) table else s"${AppConfig.DB_SCHEMA}.$table"
  }

  def save(df: DataFrame, table: String): Unit = {
    val tableName = qualify(table)
    df.write
      .mode(SaveMode.Overwrite)
      .jdbc(AppConfig.DB_URL, tableName, jdbcProps())
  }

  def saveBronze(df: DataFrame): Unit = save(df, AppConfig.Tables.BRONZE)
  def saveSilver(df: DataFrame): Unit = save(df, AppConfig.Tables.SILVER)
  def saveDim(df: DataFrame, table: String): Unit = save(df, table)
  def saveFact(df: DataFrame): Unit = save(df, AppConfig.Tables.FACT)
}