package spark.task.PSR

import scala.reflect.runtime.universe

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext

class AFixLineStatD(hc: HiveContext, dataReader: PSRDataReader) extends Serializable {
  import hc.implicits._

  def statProv(linesegTotalData: DataFrame, linesegDetailData: DataFrame) = {
    stat(linesegTotalData.toDF.withColumn("PI_ORG_NO", lit("232AF1D001B65527E055000000000001")),
      linesegDetailData.toDF.withColumn("PI_ORG_NO", lit("232AF1D001B65527E055000000000001")))
  }

  def statCity(linesegTotalData: DataFrame, linesegDetailData: DataFrame) = {
    val CITY = (df: DataFrame) => {
      df.join(dataReader.city, $"CITY" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").unionAll(
        df.join(dataReader.townOfCity, $"TOWN" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").drop("TOWN")).unionAll(
          df.join(dataReader.bzOfCity, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").drop("BZ"))
    }

    stat(CITY(linesegTotalData), CITY(linesegDetailData))
  }

  def statTown(linesegTotalData: DataFrame, linesegDetailData: DataFrame) = {
    val TOWN = (df: DataFrame) => {
      df.join(dataReader.town, $"TOWN" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"TOWN").drop("TOWN").unionAll(
        df.join(dataReader.bzOfTown, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"TOWN").drop("TOWN").drop("BZ"))
    }

    stat(TOWN(linesegTotalData), TOWN(linesegDetailData))
  }

  def statBZ(linesegTotalData: DataFrame, linesegDetailData: DataFrame) = {
    val BZ = (df: DataFrame) => {
      df.join(dataReader.bz, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"BZ").drop("BZ")
    }

    stat(BZ(linesegTotalData), BZ(linesegDetailData))
  }

  def stat(linesegTotalData: DataFrame, linesegDetailData: DataFrame) = {
    val linesegTotal = linesegTotalData.groupBy($"PI_ORG_NO").agg(
      "PLAN_POWEROFF_CNT" -> "sum",
      "PLAN_POWEROFF_DURATION" -> "sum",
      "TEMP_POWEROFF_CNT" -> "sum",
      "TEMP_POWEROFF_DURATION" -> "sum",
      "PROD_POWEROFF_CNT" -> "sum",
      "PROD_POWEROFF_DURATION" -> "sum").withColumnRenamed("sum(PLAN_POWEROFF_CNT)", "LINESEG_PLAN_POWEROFF_CNT").withColumnRenamed("sum(PLAN_POWEROFF_DURATION)", "LINESEG_PLAN_POWEROFF_DURATION")
      .withColumnRenamed("sum(TEMP_POWEROFF_CNT)", "LINESEG_TEMP_POWEROFF_CNT")
      .withColumnRenamed("sum(TEMP_POWEROFF_DURATION)", "LINESEG_TEMP_POWEROFF_DURATION")
      .withColumnRenamed("sum(PROD_POWEROFF_CNT)", "LINESEG_PROD_POWEROFF_CNT").withColumnRenamed("sum(PROD_POWEROFF_DURATION)", "LINESEG_PROD_POWEROFF_DURATION")
      

    val linesegDetailPlan = linesegDetailData.select("PI_ORG_NO", "LINE_SEG_NO", "POWER_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1).groupBy($"PI_ORG_NO", $"LINE_SEG_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "LINESEG_REPOWEROFF_PLAN_CNT")

    val linesegDetailTemp = linesegDetailData.select("PI_ORG_NO", "LINE_SEG_NO", "PLAN_OFF_TYPE").filter($"PLAN_OFF_TYPE" === 2).groupBy($"PI_ORG_NO", $"LINE_SEG_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "LINESEG_REPOWEROFF_TEMP_CNT")

    val linesegDetailFault = linesegDetailData.select("PI_ORG_NO", "LINE_SEG_NO", "POWER_OFF_TYPE").filter($"POWER_OFF_TYPE" === 2).groupBy($"PI_ORG_NO", $"LINE_SEG_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "LINESEG_REPOWEROFF_FAULT_CNT")

    linesegTotal.join(linesegDetailPlan, Seq("PI_ORG_NO"), "outer").join(linesegDetailTemp, Seq("PI_ORG_NO"), "outer")
      .join(linesegDetailFault, Seq("PI_ORG_NO"), "outer")
  }
}