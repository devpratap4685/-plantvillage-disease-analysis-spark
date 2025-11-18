package com.plantvillage.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.nio.file.{Files, Paths}
import java.time.Instant
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode
import java.sql.Timestamp

case class ImageMeta(
                      image_id: Long,
                      path_str: String,
                      filename: String,
                      folder_name: String,
                      file_size_kb: Double,
                      extract_ts: Timestamp
                    )

object Extract {

  // Driver-side file listing using Java NIO (works on Windows without winutils)
  def run(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val basePath = Paths.get(AppConfig.INPUT_PATH)
    if (!Files.exists(basePath)) {
      throw new IllegalArgumentException(s"Input path does not exist: ${AppConfig.INPUT_PATH}")
    }

    // walk tree and collect regular files
    val stream = Files.walk(basePath)
    val paths = try {
      stream.iterator().asScala.filter(p => Files.isRegularFile(p)).toList
    } finally {
      stream.close()
    }

    // map to ImageMeta (compute file size and metadata)
    val nowTs = Timestamp.from(Instant.now())
    val rawSeq = paths.map { p =>
      val pathStr = p.toAbsolutePath.toString.replace("\\", "/")
      val filename = Option(p.getFileName).map(_.toString).getOrElse("")
      val folderName = Option(p.getParent).flatMap(pp => Option(pp.getFileName)).map(_.toString).getOrElse("unknown")
      val sizeKb = try {
        val kb = Files.size(p).toDouble / 1024.0
        // use Scala BigDecimal rounding mode
        BigDecimal(kb).setScale(2, RoundingMode.HALF_UP).toDouble
      } catch {
        case _: Throwable => 0.0
      }
      ImageMeta(0L, pathStr, filename, folderName, sizeKb, nowTs)
    }

    // assign deterministic ids on driver
    val withIds = rawSeq.zipWithIndex.map { case (m, idx) => m.copy(image_id = idx.toLong) }

    // create DataFrame
    val df = spark.createDataset(withIds).toDF()

    // optional: filter out zero-sized files (if desired)
    df.filter($"file_size_kb" > 0.0)
  }
}