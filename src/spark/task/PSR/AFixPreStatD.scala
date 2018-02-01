package spark.task.PSR

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._

class AFixPreStatD(hc: HiveContext, dataReader: PSRDataReader) extends Serializable {
  import hc.implicits._

  private val areaMap = Map[String, String]("2" -> "('0')", "3" -> "('1')")
  private var allArea: (String, (DataFrame, DataFrame, DataFrame, DataFrame)) = ("1", (null, null, null, null))
  private var areaMapData: Map[String, (DataFrame, DataFrame, DataFrame, DataFrame)] = null

  private val ural = dataReader.ST_TQ_BYQ.select($"PMS_BYQ_BS" as "TG_ID", $"CNW" as "URAL_FLAG").distinct().persist
  private val consUral = dataReader.CONSDET.select($"CONS_NO", $"CNW" as "URAL_FLAG").distinct().persist

  def setData(tgTotalData: DataFrame, tgDetailData: DataFrame, consTotalData: DataFrame, consDetailData: DataFrame) {
    areaMapData = areaMap.map {
      x =>
        {
          x._1 -> (tgTotalData.join(ural.filter(s"URAL_FLAG in ${x._2}"), Seq("TG_ID")).persist, tgDetailData.join(ural.filter(s"URAL_FLAG in ${x._2}"), Seq("TG_ID")).persist,
                    consTotalData.join(consUral.filter(s"URAL_FLAG in ${x._2}"), Seq("CONS_NO")).persist, consDetailData.join(consUral.filter(s"URAL_FLAG in ${x._2}"), Seq("CONS_NO")).persist)
        }
    }
    allArea = (allArea._1, (tgTotalData, tgDetailData, consTotalData, consDetailData))
  }

  def clearData {
    ural.unpersist
    areaMapData.foreach { x =>
      x._2._1.unpersist()
      x._2._2.unpersist()
      x._2._3.unpersist()
      x._2._4.unpersist()
    }
  }

  def statProv = {
    statByArea(lit("232AF1D001B65527E055000000000001"))
  }

  def statCity = {
    val CITY = (df: DataFrame) => {
      df.join(dataReader.city, $"CITY" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").unionAll(
        df.join(dataReader.townOfCity, $"TOWN" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").drop("TOWN")).unionAll(
          df.join(dataReader.bzOfCity, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").drop("BZ"))
    }

    statByArea(CITY)
  }

  def statTown = {
    val TOWN = (df: DataFrame) => {
      df.join(dataReader.town, $"TOWN" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"TOWN").drop("TOWN").unionAll(
        df.join(dataReader.bzOfTown, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"TOWN").drop("TOWN").drop("BZ"))
    }

    statByArea(TOWN)
  }

  def statBZ = {
    val BZ = (df: DataFrame) => {
      df.join(dataReader.bz, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"BZ").drop("BZ")
    }

    statByArea(BZ)
  }

  def statByArea(col: Column) = {
    var data = stat(
        allArea._2._1.withColumn("PI_ORG_NO", col), 
        allArea._2._2.withColumn("PI_ORG_NO", col),
        allArea._2._3.withColumn("PI_ORG_NO", col),
        allArea._2._4.withColumn("PI_ORG_NO", col)).withColumn("AREA_TYPE_CODE", lit(allArea._1)) // 全口径
    for (area <- areaMapData)
      data = data.unionAll(stat(area._2._1.withColumn("PI_ORG_NO", col), area._2._2.withColumn("PI_ORG_NO", col),
                          area._2._3.withColumn("PI_ORG_NO", col), area._2._4.withColumn("PI_ORG_NO", col)).withColumn("AREA_TYPE_CODE", lit(area._1)))
    data
  }

  def statByArea(filterOrg: (DataFrame) => DataFrame) = {
    
    var data = stat(filterOrg(allArea._2._1), filterOrg(allArea._2._2), filterOrg(allArea._2._3), filterOrg(allArea._2._4)).withColumn("AREA_TYPE_CODE", lit(allArea._1)) // 全口径
    for (area <- areaMapData)
      data = data.unionAll(stat(filterOrg(area._2._1), filterOrg(area._2._2), filterOrg(area._2._3), filterOrg(area._2._4)).withColumn("AREA_TYPE_CODE", lit(area._1)))
    data
  }

  def stat(tgTotalData: DataFrame, tgDetailData: DataFrame, consTotalData: DataFrame, consDetailData: DataFrame) = {
    //    val DYYHJCGYPBSL = s.groupBy("PMS_DWBM").agg(countDistinct($"SSPB") as "DYYHJCGYPBSL")

    val tgTotal = tgTotalData.groupBy("PI_ORG_NO").agg(
        sum($"PLAN_POWEROFF_CNT") as "M_PLAN_POWEROFF_CNT",
        sum($"PLAN_POWEROFF_DURATION") as "M_PLAN_POWEROFF_DURATION",
        sum($"PLAN_LOSS_POWER") as "M_PLAN_LOSS_POWER",
        sum($"TEMP_POWEROFF_CNT") as "M_TEMP_POWEROFF_CNT",
        sum($"TEMP_POWEROFF_DURATION") as "M_TEMP_POWEROFF_DURATION",
        sum($"TEMP_LOSS_POWER") as "M_TEMP_LOSS_POWER",
        sum($"PROD_POWEROFF_CNT") as "M_PROD_POWEROFF_CNT",
        sum($"PROD_POWEROFF_DURATION") as "M_PROD_POWEROFF_DURATION",
        sum($"PROD_LOSS_POWER") as "M_PROD_LOSS_POWER")

    val tgDetailAll = tgDetailData.select("PI_ORG_NO", "TG_ID")
      .groupBy($"PI_ORG_NO", $"TG_ID").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "TG_REPOWEROFF_All_CNT")
      
    val tgDetailPlan = tgDetailData.select("PI_ORG_NO", "TG_ID", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 1)
      .groupBy($"PI_ORG_NO", $"TG_ID").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "TG_REPOWEROFF_PLAN_CNT")

    val tgDetailFault = tgDetailData.select("PI_ORG_NO", "TG_ID", "POWER_OFF_TYPE").filter($"POWER_OFF_TYPE" === 2)
       .groupBy($"PI_ORG_NO", $"TG_ID").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "TG_REPOWEROFF_FAULT_CNT")
      
    val tgDetailTemp = tgDetailData.select("PI_ORG_NO", "TG_ID", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO", $"TG_ID").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "TG_REPOWEROFF_TEMP_CNT")
      
    val tgDetailPlanCnt = tgDetailData.select("PI_ORG_NO", "TG_ID", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 1)
      .groupBy($"PI_ORG_NO").agg(countDistinct($"TG_ID") as "ZYJHTD_PBSL")
      
    val tgDetailFaultCnt = tgDetailData.select("PI_ORG_NO", "TG_ID", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO").agg(countDistinct($"TG_ID") as "ZYGZTD_PBSL")
      
    val tgDetailTempCnt = tgDetailData.select("PI_ORG_NO", "TG_ID", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO").agg(countDistinct($"TG_ID") as "ZYLSJXTD_PBSL")

      
    val consTotal = consTotalData.groupBy($"PI_ORG_NO").agg(
        sum($"PLAN_POWEROFF_CNT") as "L_PLAN_POWEROFF_CNT",
        sum($"PLAN_POWEROFF_DURATION") as "L_PLAN_POWEROFF_DURATION",
        sum($"PLAN_LOSS_POWER") as "L_PLAN_LOSS_POWER",
        sum($"TEMP_POWEROFF_CNT") as "L_TEMP_POWEROFF_CNT",
        sum($"TEMP_POWEROFF_DURATION") as "L_TEMP_POWEROFF_DURATION",
        sum($"TEMP_LOSS_POWER") as "L_TEMP_LOSS_POWER",
        sum($"PROD_POWEROFF_CNT") as "L_PROD_POWEROFF_CNT",
        sum($"PROD_POWEROFF_DURATION") as "L_PROD_POWEROFF_DURATION",
        sum($"PROD_LOSS_POWER") as "L_PROD_LOSS_POWER")

    val consDetailAll = consDetailData.select("PI_ORG_NO", "CONS_NO")
      .groupBy($"PI_ORG_NO", $"CONS_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "CONS_REPOWEROFF_All_CNT")
      
    val consDetailPlan = consDetailData.select("PI_ORG_NO", "CONS_NO", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 1)
      .groupBy($"PI_ORG_NO", $"CONS_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "CONS_REPOWEROFF_PLAN_CNT")

    val consDetailFault = consDetailData.select("PI_ORG_NO", "CONS_NO", "POWER_OFF_TYPE").filter($"POWER_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO", $"CONS_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "CONS_REPOWEROFF_FAULT_CNT")
      
    val consDetailTemp = consDetailData.select("PI_ORG_NO", "CONS_NO", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO", $"CONS_NO").count.filter($"count" > 1)
      .groupBy($"PI_ORG_NO").count.withColumnRenamed("count", "CONS_REPOWEROFF_TEMP_CNT")
      
    val consDetailPlanCnt = consDetailData.select("PI_ORG_NO", "CONS_NO", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 1)
      .groupBy($"PI_ORG_NO").agg(countDistinct($"CONS_NO") as "DYJHTD_YHSL")

    val consDetailFaultCnt = consDetailData.select("PI_ORG_NO", "CONS_NO", "POWER_OFF_TYPE").filter($"POWER_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO").agg(countDistinct($"CONS_NO") as "DYGZTD_YHSL")
      
    val consDetailTempCnt = consDetailData.select("PI_ORG_NO", "CONS_NO", "POWER_OFF_TYPE", "PLAN_OFF_TYPE").filter($"POWER_OFF_TYPE" === 1 and $"PLAN_OFF_TYPE" === 2)
      .groupBy($"PI_ORG_NO").agg(countDistinct($"CONS_NO") as "DYLSJXTD_YHSL")

    transColumn(tgTotal.join(tgDetailAll, Seq("PI_ORG_NO"), "outer")
          .join(tgDetailPlan, Seq("PI_ORG_NO"), "outer").join(tgDetailTemp, Seq("PI_ORG_NO"), "outer").join(tgDetailFault, Seq("PI_ORG_NO"), "outer")
          .join(tgDetailPlanCnt, Seq("PI_ORG_NO"), "outer").join(tgDetailTempCnt, Seq("PI_ORG_NO"), "outer").join(tgDetailFaultCnt, Seq("PI_ORG_NO"), "outer")
          .join(consTotal, Seq("PI_ORG_NO"), "outer")
          .join(consDetailAll, Seq("PI_ORG_NO"), "outer")
          .join(consDetailFault, Seq("PI_ORG_NO"), "outer").join(consDetailPlan, Seq("PI_ORG_NO"), "outer").join(consDetailTemp, Seq("PI_ORG_NO"), "outer")
          .join(consDetailFaultCnt, Seq("PI_ORG_NO"), "outer").join(consDetailPlanCnt, Seq("PI_ORG_NO"), "outer").join(consDetailTempCnt, Seq("PI_ORG_NO"), "outer"))
  }
  
  def transColumn(df : DataFrame) : DataFrame ={
    df.select(
        $"PI_ORG_NO",
        $"TG_REPOWEROFF_All_CNT",
        $"CONS_REPOWEROFF_All_CNT",
        
        $"M_PLAN_POWEROFF_CNT",
        $"M_PLAN_POWEROFF_DURATION",
        $"M_PLAN_LOSS_POWER",
        $"ZYJHTD_PBSL",
        $"TG_REPOWEROFF_PLAN_CNT",
        
        $"M_PROD_POWEROFF_CNT",
        $"M_PROD_POWEROFF_DURATION",
        $"M_PROD_LOSS_POWER",
        $"ZYGZTD_PBSL",
        $"TG_REPOWEROFF_FAULT_CNT",
        
        $"M_TEMP_POWEROFF_CNT",
        $"M_TEMP_POWEROFF_DURATION",
        $"M_TEMP_LOSS_POWER",
        $"ZYLSJXTD_PBSL",
        $"TG_REPOWEROFF_TEMP_CNT",
        
        $"L_PLAN_POWEROFF_CNT",
        $"L_PLAN_POWEROFF_DURATION",
        $"L_PLAN_LOSS_POWER",
        $"DYJHTD_YHSL",
        $"CONS_REPOWEROFF_PLAN_CNT",
        
        $"L_TEMP_POWEROFF_CNT",
        $"L_TEMP_POWEROFF_DURATION",
        $"L_TEMP_LOSS_POWER",
        $"DYLSJXTD_YHSL",
        $"CONS_REPOWEROFF_TEMP_CNT",
        
        $"L_PROD_POWEROFF_CNT",
        $"L_PROD_POWEROFF_DURATION",
        $"L_PROD_LOSS_POWER",
        $"DYGZTD_YHSL",
        $"CONS_REPOWEROFF_FAULT_CNT" 
        )
    
//    df.select(
//        $"PI_ORG_NO",
//        $"TG_REPOWEROFF_All_CNT" as "ZYCFTD_PBSL",
//        $"CONS_REPOWEROFF_All_CNT" as "ZYCFTD_YHSL",
//        
//        $"M_PLAN_POWEROFF_CNT" as "ZYJHTDCS",
//        $"M_PLAN_POWEROFF_DURATION" as "ZYJHTDSC",
//        $"M_PLAN_LOSS_POWER" as "ZYJHTDYXDL",
//        $"ZYJHTD_PBSL" as "ZYJHTD_PBSL",
//        $"TG_REPOWEROFF_PLAN_CNT" as "ZYJHCFTD_PBSL",
//        
//        $"M_PROD_POWEROFF_CNT" as "ZYGZTDCS",
//        $"M_PROD_POWEROFF_DURATION" as "ZYGZTDSC",
//        $"M_PROD_LOSS_POWER" as "ZYGZTDYXDL",
//        $"ZYGZTD_PBSL" as "ZYGZTD_PBSL",
//        $"TG_REPOWEROFF_FAULT_CNT" as "ZYGZCFTD_PBSL",
//        
//        $"M_TEMP_POWEROFF_CNT" as "ZYLSJXTDCS",
//        $"M_TEMP_POWEROFF_DURATION" as "ZYLSJXTDSC",
//        $"M_TEMP_LOSS_POWER" as "ZYLSJXYXDL",
//        $"ZYLSJXTD_PBSL" as "ZYLSJXTD_PBSL",
//        $"TG_REPOWEROFF_TEMP_CNT" as "ZYLSJXCFTD_PBSL",
//        
//        $"L_PLAN_POWEROFF_CNT" as "DYJHTDCS",
//        $"L_PLAN_POWEROFF_DURATION" as "DYJHTDSC",
//        $"L_PLAN_LOSS_POWER" as "DYJHTDYXDL",
//        $"DYJHTD_YHSL" as "DYJHTD_YHSL",
//        $"CONS_REPOWEROFF_PLAN_CNT" as "DYJHCFTD_YHSL",
//        
//        $"L_TEMP_POWEROFF_CNT" as "DYLSJXTDCS",
//        $"L_TEMP_POWEROFF_DURATION" as "DYLSJXTDSC",
//        $"L_TEMP_LOSS_POWER" as "DYLSJXYXDL",
//        $"DYLSJXTD_YHSL" as "DYLSJXTD_YHSL",
//        $"CONS_REPOWEROFF_TEMP_CNT" as "DYLSJXCFTD_YHSL",
//        
//        $"L_PROD_POWEROFF_CNT" as "DYGZTDCS",
//        $"L_PROD_POWEROFF_DURATION" as "DYGZTDSC",
//        $"L_PROD_LOSS_POWER" as "DYGZTDYXDL",
//        $"DYGZTD_YHSL" as "DYGZTD_YHSL",
//        $"CONS_REPOWEROFF_FAULT_CNT" as "DYGZCFTD_YHSL"
//        )
  }
}