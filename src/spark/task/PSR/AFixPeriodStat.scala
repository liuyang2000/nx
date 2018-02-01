package spark.task.PSR

import java.util.Calendar
import java.util.Date
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import spark.util.NullableStatProcess
import spark.util.utils
import org.apache.spark.storage.StorageLevel
import spark.moudel.ALinesegTotalStatDRow

case class AFixPreStatDRow(val PI_ORG_NO : java.lang.String, val M_PLAN_POWEROFF_CNT: java.lang.Long, val M_PROD_POWEROFF_DURATION: java.lang.Double,
                           val M_TEMP_POWEROFF_DURATION: java.lang.Double, val M_TEMP_LOSS_POWER: java.lang.Double, 
                           val M_PLAN_POWEROFF_DURATION: java.lang.Double, val M_PLAN_LOSS_POWER: java.lang.Double, 
                           val M_PROD_LOSS_POWER: java.lang.Double, val M_PROD_POWEROFF_CNT: java.lang.Long, 
                           val M_TEMP_POWEROFF_CNT: java.lang.Long, val TG_REPOWEROFF_PLAN_CNT: java.lang.Long,
                           val TG_REPOWEROFF_FAULT_CNT: java.lang.Long, val AREA_TYPE_CODE: String,
                           val TG_REPOWEROFF_TEMP_CNT : java.lang.Long, val TG_REPOWEROFF_All_CNT : java.lang.Long, val ZYJHTD_PBSL : java.lang.Long, 
                           val ZYGZTD_PBSL : java.lang.Long, val ZYLSJXTD_PBSL : java.lang.Long)
              
                           
case class AFixConsStatDRow(val PI_ORG_NO: String, val L_PLAN_POWEROFF_CNT: java.lang.Long, val L_PROD_POWEROFF_DURATION: java.lang.Double,
                           val L_TEMP_POWEROFF_DURATION: java.lang.Double, val L_TEMP_LOSS_POWER: java.lang.Double, 
                           val L_PLAN_POWEROFF_DURATION: java.lang.Double, val L_PLAN_LOSS_POWER: java.lang.Double, 
                           val L_PROD_LOSS_POWER: java.lang.Double, val L_PROD_POWEROFF_CNT: java.lang.Long, 
                           val L_TEMP_POWEROFF_CNT: java.lang.Long, val CONS_REPOWEROFF_PLAN_CNT: java.lang.Long,
                           val CONS_REPOWEROFF_FAULT_CNT: java.lang.Long, val AREA_TYPE_CODE: String,
                           val CONS_REPOWEROFF_TEMP_CNT : java.lang.Long, val CONS_REPOWEROFF_All_CNT : java.lang.Long, val DYJHTD_YHSL : java.lang.Long, 
                           val DYGZTD_YHSL : java.lang.Long, val DYLSJXTD_YHSL : java.lang.Long)

case class AFixLineStatDRow(val PI_ORG_NO: String, val LINESEG_PLAN_POWEROFF_CNT: java.lang.Long, val LINESEG_PROD_POWEROFF_DURATION: java.lang.Double,
                            val LINESEG_TEMP_POWEROFF_DURATION: java.lang.Double, val LINESEG_PLAN_POWEROFF_DURATION: java.lang.Double,
                            val LINESEG_PROD_POWEROFF_CNT: java.lang.Long,
                            val LINESEG_TEMP_POWEROFF_CNT: java.lang.Long, val LINESEG_REPOWEROFF_PLAN_CNT: java.lang.Long, val LINESEG_REPOWEROFF_TEMP_CNT: java.lang.Long,
                            val LINESEG_REPOWEROFF_FAULT_CNT: java.lang.Long)

class AFixPeriodStat(hc: HiveContext, dataReader: PSRDataReader, aFixPreOrg: DataFrame, aFixPreBr: DataFrame, aFixLineOrg: DataFrame, aFixLineBr: DataFrame, isInit: String, isBatch: String) extends Serializable with NullableStatProcess {
  import hc.implicits._

  private val date = dataReader.date
  private val init = if (isInit == "init") true else false
  private val isWrite = if (isBatch == "batch") false else true
  private val calendar = Calendar.getInstance
  calendar.setTime(date)

  private val tableArray = Array("PWYW.PWYW_DWGDKKXTJ_SS_Z", "PWYW.PWYW_DWGDKKXTJ_BZ_Z", "PWYW.PWYW_DWXLTDTJ_SS_Z", "PWYW.PWYW_DWXLTDTJ_BZ_Z",
    "PWYW.PWYW_DWGDKKXTJ_SS_Y", "PWYW.PWYW_DWGDKKXTJ_BZ_Y", "PWYW.PWYW_DWXLTDTJ_SS_Y", "PWYW.PWYW_DWXLTDTJ_BZ_Y",
    "PWYW.PWYW_DWGDKKXTJ_SS_J", "PWYW.PWYW_DWGDKKXTJ_BZ_J", "PWYW.PWYW_DWXLTDTJ_SS_J", "PWYW.PWYW_DWXLTDTJ_BZ_J",
    "PWYW.PWYW_DWGDKKXTJ_SS_BN", "PWYW.PWYW_DWGDKKXTJ_BZ_BN", "PWYW.PWYW_DWXLTDTJ_SS_BN", "PWYW.PWYW_DWXLTDTJ_BZ_BN",
    "PWYW.PWYW_DWGDKKXTJ_SS_N", "PWYW.PWYW_DWGDKKXTJ_BZ_N", "PWYW.PWYW_DWXLTDTJ_SS_N", "PWYW.PWYW_DWXLTDTJ_BZ_N")

  private val tableRBTMP = tableArray.map {
    r =>
      {
        if(!init) hc.table(r).write.mode("overwrite").saveAsTable(r + "_RBTMP")
        r + "_RBTMP"
      }
  }

  def clean {
    if(!init) tableRBTMP.foreach(r => hc.sql("DROP TABLE " + r))
  }

  def rollBack {
    println("afix rollBack")
    if(!init) tableRBTMP.foreach(r => hc.table(r).write.mode("overwrite").saveAsTable(r.dropRight(6)))
  }
  
  def selTgCol(tt : DataFrame) : DataFrame = {
    tt.select($"PI_ORG_NO", $"M_PLAN_POWEROFF_CNT", $"M_PROD_POWEROFF_DURATION",
                           $"M_TEMP_POWEROFF_DURATION", $"M_TEMP_LOSS_POWER", 
                           $"M_PLAN_POWEROFF_DURATION", $"M_PLAN_LOSS_POWER", 
                           $"M_PROD_LOSS_POWER", $"M_PROD_POWEROFF_CNT", 
                           $"M_TEMP_POWEROFF_CNT", $"TG_REPOWEROFF_PLAN_CNT",
                           $"TG_REPOWEROFF_FAULT_CNT", $"AREA_TYPE_CODE",
                           $"TG_REPOWEROFF_TEMP_CNT", $"TG_REPOWEROFF_All_CNT", $"ZYJHTD_PBSL", 
                           $"ZYGZTD_PBSL", $"ZYLSJXTD_PBSL")
  }
  
  def selConsCol(tt : DataFrame) : DataFrame = {
      tt.select($"PI_ORG_NO", $"L_PLAN_POWEROFF_CNT", $"L_PROD_POWEROFF_DURATION",
                           $"L_TEMP_POWEROFF_DURATION", $"L_TEMP_LOSS_POWER", 
                           $"L_PLAN_POWEROFF_DURATION", $"L_PLAN_LOSS_POWER", 
                           $"L_PROD_LOSS_POWER", $"L_PROD_POWEROFF_CNT", 
                           $"L_TEMP_POWEROFF_CNT", $"CONS_REPOWEROFF_PLAN_CNT",
                           $"CONS_REPOWEROFF_FAULT_CNT", $"AREA_TYPE_CODE",
                           $"CONS_REPOWEROFF_TEMP_CNT", $"CONS_REPOWEROFF_All_CNT", $"DYJHTD_YHSL", 
                           $"DYGZTD_YHSL", $"DYLSJXTD_YHSL")
  }

  def statWeek {
    val calendar1 = Calendar.getInstance
    calendar1.setTime(date)
    calendar1.set(Calendar.DAY_OF_WEEK, 2)
    val STAT_DATE_S = utils.sdfDay.format(calendar1.getTime)
    calendar1.add(Calendar.WEEK_OF_YEAR, 1)
    calendar1.set(Calendar.DAY_OF_WEEK, 8)
    val STAT_DATE_E = utils.sdfDay.format(calendar1.getTime)

    val dow = calendar.get(Calendar.DAY_OF_WEEK)
    val ifWrite = isWrite || dow == 1

    if (init || dow == 2) {
      aFixPreOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_Z")
      if (ifWrite) PSRToOracle.aFixPreStatW(aFixPreOrg.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_Z", STAT_DATE_S, STAT_DATE_E)
      aFixPreBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_Z")
      if (ifWrite) PSRToOracle.aFixPreStatW(aFixPreBr.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_Z", STAT_DATE_S, STAT_DATE_E)
      aFixLineOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_Z")
      if (ifWrite) PSRToOracle.aFixLineStatW(aFixLineOrg.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_Z", STAT_DATE_S, STAT_DATE_E)
      aFixLineBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_Z")
      if (ifWrite) PSRToOracle.aFixLineStatW(aFixLineBr.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_Z", STAT_DATE_S, STAT_DATE_E)
    } else {

      val ss = hc.table("PWYW.PWYW_DWGDKKXTJ_SS_Z")
      val dfSSTg = add1_tg(selTgCol(ss).as[AFixPreStatDRow], selTgCol(aFixPreOrg).as[AFixPreStatDRow]).toDF.persist
      val dfSSCons = add1_cons(selConsCol(ss).as[AFixConsStatDRow], selConsCol(aFixPreOrg).as[AFixConsStatDRow]).toDF.persist
      val df1 = transColumn(dfSSTg.join(dfSSCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_Z")
      if (ifWrite) PSRToOracle.aFixPreStatW(df1.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_Z", STAT_DATE_S, STAT_DATE_E)
      
      val bz = hc.table("PWYW.PWYW_DWGDKKXTJ_BZ_Z")
      val dfBZTg = add1_tg(selTgCol(bz).as[AFixPreStatDRow], selTgCol(aFixPreBr).as[AFixPreStatDRow]).toDF.persist
      val dfBZCons = add1_cons(selConsCol(bz).as[AFixConsStatDRow], selConsCol(aFixPreBr).as[AFixConsStatDRow]).toDF.persist
      val df2 = transColumn(dfBZTg.join(dfBZCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_Z")
      if (ifWrite) PSRToOracle.aFixPreStatW(df2.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_Z", STAT_DATE_S, STAT_DATE_E)
      
      val df3 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_SS_Z").as[AFixLineStatDRow], aFixLineOrg.as[AFixLineStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_Z")
      if (ifWrite) PSRToOracle.aFixLineStatW(df3.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_Z", STAT_DATE_S, STAT_DATE_E)
      
      val df4 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_BZ_Z").as[AFixLineStatDRow], aFixLineBr.as[AFixLineStatDRow]).toDF.persist
      df4.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_Z")
      if (ifWrite) PSRToOracle.aFixLineStatW(df4.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_Z", STAT_DATE_S, STAT_DATE_E)
      df1.unpersist(); df2.unpersist(); df3.unpersist(); df4.unpersist();
    }
  }

  def statMonth {
    val STAT_DATE = utils.sdfMonth.format(date)

    val dom = calendar.get(Calendar.DAY_OF_MONTH)
    val maxDom = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    val ifWrite = isWrite || dom == maxDom

    if (init || dom == 1) {
      aFixPreOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_Y")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreOrg.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_Y", STAT_DATE, false)
      aFixPreBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_Y")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreBr.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_Y", STAT_DATE, false)
      aFixLineOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_Y")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineOrg.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_Y", STAT_DATE, false)
      aFixLineBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_Y")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineBr.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_Y", STAT_DATE, false)
    } else {
      val ss = hc.table("PWYW.PWYW_DWGDKKXTJ_SS_Y")
      val dfSSTg = add1_tg(selTgCol(ss).as[AFixPreStatDRow], selTgCol(aFixPreOrg).as[AFixPreStatDRow]).toDF.persist
      val dfSSCons = add1_cons(selConsCol(ss).as[AFixConsStatDRow], selConsCol(aFixPreOrg).as[AFixConsStatDRow]).toDF.persist
      val df1 = transColumn(dfSSTg.join(dfSSCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_Y")
      if (ifWrite) PSRToOracle.aFixPreStat(df1.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_Y", STAT_DATE, false)
      
      val bz = hc.table("PWYW.PWYW_DWGDKKXTJ_BZ_Y")
      val dfBZTg = add1_tg(selTgCol(bz).as[AFixPreStatDRow], selTgCol(aFixPreBr).as[AFixPreStatDRow]).toDF.persist
      val dfBZCons = add1_cons(selConsCol(bz).as[AFixConsStatDRow], selConsCol(aFixPreBr).as[AFixConsStatDRow]).toDF.persist
      val df2 = transColumn(dfBZTg.join(dfBZCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_Y")
      if (ifWrite) PSRToOracle.aFixPreStat(df2.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_Y", STAT_DATE, false)
      
      val df3 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_SS_Y").as[AFixLineStatDRow], aFixLineOrg.as[AFixLineStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_Y")
      if (ifWrite) PSRToOracle.aFixLineStat(df3.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_Y", STAT_DATE, false)
      val df4 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_BZ_Y").as[AFixLineStatDRow], aFixLineBr.as[AFixLineStatDRow]).toDF.persist
      df4.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_Y")
      if (ifWrite) PSRToOracle.aFixLineStat(df4.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_Y", STAT_DATE, false)
      df1.unpersist(); df2.unpersist(); df3.unpersist(); df4.unpersist();
    }
  }

  def statSeason {
    val year = utils.sdfYear.format(date)
    val month = utils.sdfMonth.format(date).takeRight(2)
    val STAT_DATE = month match {
      case _ if month == "01" || month == "02" || month == "03" => year + "01"
      case _ if month == "04" || month == "05" || month == "06" => year + "02"
      case _ if month == "07" || month == "08" || month == "09" => year + "03"
      case _ if month == "10" || month == "11" || month == "12" => year + "04"
    }

    val dom = calendar.get(Calendar.DAY_OF_MONTH)
    val maxDom = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    val isLastMonth = month == "03" || month == "06" || month == "07" || month == "12"
    val ifWrite = isWrite || (isLastMonth && dom == maxDom)

    if (init || dom == 1 && (month == "01" || month == "04" || month == "07" || month == "10")) {
      aFixPreOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_J")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreOrg.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_J", STAT_DATE, false)
      aFixPreBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_J")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreBr.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_J", STAT_DATE, false)
      aFixLineOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_J")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineOrg.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_J", STAT_DATE, false)
      aFixLineBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_J")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineBr.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_J", STAT_DATE, false)
    } else {
      val ss = hc.table("PWYW.PWYW_DWGDKKXTJ_SS_J")
      val dfSSTg = add1_tg(selTgCol(ss).as[AFixPreStatDRow], selTgCol(aFixPreOrg).as[AFixPreStatDRow]).toDF.persist
      val dfSSCons = add1_cons(selConsCol(ss).as[AFixConsStatDRow], selConsCol(aFixPreOrg).as[AFixConsStatDRow]).toDF.persist
      val df1 = transColumn(dfSSTg.join(dfSSCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_J")
      if (ifWrite) PSRToOracle.aFixPreStat(df1.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_J", STAT_DATE, false)
      
      val bz = hc.table("PWYW.PWYW_DWGDKKXTJ_BZ_J")
      val dfBZTg = add1_tg(selTgCol(bz).as[AFixPreStatDRow], selTgCol(aFixPreBr).as[AFixPreStatDRow]).toDF.persist
      val dfBZCons = add1_cons(selConsCol(bz).as[AFixConsStatDRow], selConsCol(aFixPreBr).as[AFixConsStatDRow]).toDF.persist
      val df2 = transColumn(dfBZTg.join(dfBZCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_J")
      if (ifWrite) PSRToOracle.aFixPreStat(df2.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_J", STAT_DATE, false)
      val df3 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_SS_J").as[AFixLineStatDRow], aFixLineOrg.as[AFixLineStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_J")
      if (ifWrite) PSRToOracle.aFixLineStat(df3.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_J", STAT_DATE, false)
      val df4 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_BZ_J").as[AFixLineStatDRow], aFixLineBr.as[AFixLineStatDRow]).toDF.persist
      df4.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_J")
      if (ifWrite) PSRToOracle.aFixLineStat(df4.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_J", STAT_DATE, false)
      df1.unpersist(); df2.unpersist(); df3.unpersist(); df4.unpersist();
    }
  }

  def statHY {
    val year = utils.sdfYear.format(date)
    val month = utils.sdfMonth.format(date).takeRight(2)
    val STAT_DATE = month match {
      case _ if month == "01" || month == "02" || month == "03" || month == "04" || month == "05" || month == "06" => year + "01"
      case _ if month == "07" || month == "08" || month == "09" || month == "10" || month == "11" || month == "12" => year + "02"
    }

    val dom = calendar.get(Calendar.DAY_OF_MONTH)
    val maxDom = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    val isLastMonth = month == "06" || month == "12"
    def ifWrite = isWrite || (isLastMonth && dom == maxDom)

    if (init || dom == 1 && (month == "01" || month == "07")) {
      aFixPreOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_BN")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreOrg.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_BN", STAT_DATE, false)
      aFixPreBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_BN")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreBr.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_BN", STAT_DATE, false)
      aFixLineOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_BN")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineOrg.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_BN", STAT_DATE, false)
      aFixLineBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_BN")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineBr.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_BN", STAT_DATE, false)
    } else {
      val ss = hc.table("PWYW.PWYW_DWGDKKXTJ_SS_BN")
      val dfSSTg = add1_tg(selTgCol(ss).as[AFixPreStatDRow], selTgCol(aFixPreOrg).as[AFixPreStatDRow]).toDF.persist
      val dfSSCons = add1_cons(selConsCol(ss).as[AFixConsStatDRow], selConsCol(aFixPreOrg).as[AFixConsStatDRow]).toDF.persist
      val df1 = transColumn(dfSSTg.join(dfSSCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_BN")
      if (ifWrite) PSRToOracle.aFixPreStat(df1.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_BN", STAT_DATE, false)
      
      val bz = hc.table("PWYW.PWYW_DWGDKKXTJ_BZ_BN")
      val dfBZTg = add1_tg(selTgCol(bz).as[AFixPreStatDRow], selTgCol(aFixPreBr).as[AFixPreStatDRow]).toDF.persist
      val dfBZCons = add1_cons(selConsCol(bz).as[AFixConsStatDRow], selConsCol(aFixPreBr).as[AFixConsStatDRow]).toDF.persist
      val df2 = transColumn(dfBZTg.join(dfBZCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_BN")
      if (ifWrite) PSRToOracle.aFixPreStat(df2.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_BN", STAT_DATE, false)
      
      val df3 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_SS_BN").as[AFixLineStatDRow], aFixLineOrg.as[AFixLineStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_BN")
      if (ifWrite) PSRToOracle.aFixLineStat(df3.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_BN", STAT_DATE, false)
      val df4 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_BZ_BN").as[AFixLineStatDRow], aFixLineBr.as[AFixLineStatDRow]).toDF.persist
      df4.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_BN")
      if (ifWrite) PSRToOracle.aFixLineStat(df4.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_BN", STAT_DATE, false)
      df1.unpersist(); df2.unpersist(); df3.unpersist(); df4.unpersist();
    }
  }

  def statYear {
    val STAT_DATE = utils.sdfYear.format(date)

    val doy = calendar.get(Calendar.DAY_OF_YEAR)
    val maxDoy = calendar.getActualMaximum(Calendar.DAY_OF_YEAR)
    val ifWrite = isWrite || doy == maxDoy

    if (init || doy == 1) {
      aFixPreOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_N")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreOrg.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_N", STAT_DATE, false)
      aFixPreBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_N")
      if (ifWrite) PSRToOracle.aFixPreStat(aFixPreBr.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_N", STAT_DATE, false)
      aFixLineOrg.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_N")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineOrg.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_N", STAT_DATE, false)
      aFixLineBr.write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_N")
      if (ifWrite) PSRToOracle.aFixLineStat(aFixLineBr.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_N", STAT_DATE, false)
    } else {
      val ss = hc.table("PWYW.PWYW_DWGDKKXTJ_SS_N")
      val dfSSTg = add1_tg(selTgCol(ss).as[AFixPreStatDRow], selTgCol(aFixPreOrg).as[AFixPreStatDRow]).toDF.persist
      val dfSSCons = add1_cons(selConsCol(ss).as[AFixConsStatDRow], selConsCol(aFixPreOrg).as[AFixConsStatDRow]).toDF.persist
      val df1 = transColumn(dfSSTg.join(dfSSCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_SS_N")
      if (ifWrite) PSRToOracle.aFixPreStat(df1.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_SS_N", STAT_DATE, false)
      
      val bz = hc.table("PWYW.PWYW_DWGDKKXTJ_BZ_N")
      val dfBZTg = add1_tg(selTgCol(bz).as[AFixPreStatDRow], selTgCol(aFixPreBr).as[AFixPreStatDRow]).toDF.persist
      val dfBZCons = add1_cons(selConsCol(bz).as[AFixConsStatDRow], selConsCol(aFixPreBr).as[AFixConsStatDRow]).toDF.persist
      val df2 = transColumn(dfBZTg.join(dfBZCons,Seq("PI_ORG_NO","AREA_TYPE_CODE"),"outer")).persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWGDKKXTJ_BZ_N")
      if (ifWrite) PSRToOracle.aFixPreStat(df2.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWGDKKXTJ_BZ_N", STAT_DATE, false)
      
      val df3 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_SS_N").as[AFixLineStatDRow], aFixLineOrg.as[AFixLineStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_SS_N")
      if (ifWrite) PSRToOracle.aFixLineStat(df3.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_SS_N", STAT_DATE, false)
      val df4 = add2(hc.table("PWYW.PWYW_DWXLTDTJ_BZ_N").as[AFixLineStatDRow], aFixLineBr.as[AFixLineStatDRow]).toDF.persist
      df4.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_DWXLTDTJ_BZ_N")
      if (ifWrite) PSRToOracle.aFixLineStat(df4.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"), "PWYW_DWXLTDTJ_BZ_N", STAT_DATE, false)
      df1.unpersist(); df2.unpersist(); df3.unpersist(); df4.unpersist();
    }
  }

  private def add1_tg(ds1: Dataset[AFixPreStatDRow], ds2: Dataset[AFixPreStatDRow]) = {
    ds1.as("a").joinWith(ds2.as("b"), $"a.PI_ORG_NO" === $"b.PI_ORG_NO" and $"a.AREA_TYPE_CODE" === $"b.AREA_TYPE_CODE", "outer").map {
      r =>
        {
          if (r._1.PI_ORG_NO == null)
            r._2
          else if (r._2.PI_ORG_NO == null)
            r._1
          else {
            val M_PLAN_POWEROFF_CNT = add(r._1.M_PLAN_POWEROFF_CNT, r._2.M_PLAN_POWEROFF_CNT)
            val M_PROD_POWEROFF_DURATION = add(r._1.M_PROD_POWEROFF_DURATION, r._2.M_PROD_POWEROFF_DURATION)
            val M_TEMP_POWEROFF_DURATION = add(r._1.M_TEMP_POWEROFF_DURATION, r._2.M_TEMP_POWEROFF_DURATION)
            val M_TEMP_LOSS_POWER = add(r._1.M_TEMP_LOSS_POWER, r._2.M_TEMP_LOSS_POWER)
            val M_PLAN_POWEROFF_DURATION = add(r._1.M_PLAN_POWEROFF_DURATION, r._2.M_PLAN_POWEROFF_DURATION)
            val M_PLAN_LOSS_POWER = add(r._1.M_PLAN_LOSS_POWER, r._2.M_PLAN_LOSS_POWER)
            val M_PROD_LOSS_POWER = add(r._1.M_PROD_LOSS_POWER, r._2.M_PROD_LOSS_POWER)
            val M_PROD_POWEROFF_CNT = add(r._1.M_PROD_POWEROFF_CNT, r._2.M_PROD_POWEROFF_CNT)
            val M_TEMP_POWEROFF_CNT = add(r._1.M_TEMP_POWEROFF_CNT, r._2.M_TEMP_POWEROFF_CNT)
            val TG_REPOWEROFF_PLAN_CNT = add(r._1.TG_REPOWEROFF_PLAN_CNT, r._2.TG_REPOWEROFF_PLAN_CNT)
            val TG_REPOWEROFF_FAULT_CNT = add(r._1.TG_REPOWEROFF_FAULT_CNT, r._2.TG_REPOWEROFF_FAULT_CNT)
            val TG_REPOWEROFF_TEMP_CNT = add(r._1.TG_REPOWEROFF_TEMP_CNT, r._2.TG_REPOWEROFF_TEMP_CNT)
            val TG_REPOWEROFF_All_CNT = add(r._1.TG_REPOWEROFF_All_CNT, r._2.TG_REPOWEROFF_All_CNT)
            val ZYJHTD_PBSL = add(r._1.ZYJHTD_PBSL, r._2.ZYJHTD_PBSL)
            val ZYGZTD_PBSL = add(r._1.ZYGZTD_PBSL, r._2.ZYGZTD_PBSL)
            val ZYLSJXTD_PBSL = add(r._1.ZYLSJXTD_PBSL, r._2.ZYLSJXTD_PBSL)
            
            AFixPreStatDRow(r._1.PI_ORG_NO, M_PLAN_POWEROFF_CNT, M_PROD_POWEROFF_DURATION, M_TEMP_POWEROFF_DURATION,
              M_TEMP_LOSS_POWER, M_PLAN_POWEROFF_DURATION, M_PLAN_LOSS_POWER, M_PROD_LOSS_POWER, M_PROD_POWEROFF_CNT,
              M_TEMP_POWEROFF_CNT, TG_REPOWEROFF_PLAN_CNT, TG_REPOWEROFF_FAULT_CNT, r._1.AREA_TYPE_CODE,
              TG_REPOWEROFF_TEMP_CNT, TG_REPOWEROFF_All_CNT, ZYJHTD_PBSL, 
                           ZYGZTD_PBSL, ZYLSJXTD_PBSL)
          }
        }
    }
  }
  
  private def add1_cons(ds1: Dataset[AFixConsStatDRow], ds2: Dataset[AFixConsStatDRow]) = {
    ds1.as("a").joinWith(ds2.as("b"), $"a.PI_ORG_NO" === $"b.PI_ORG_NO" and $"a.AREA_TYPE_CODE" === $"b.AREA_TYPE_CODE", "outer").map {
      r =>
        {
          if (r._1.PI_ORG_NO == null)
            r._2
          else if (r._2.PI_ORG_NO == null)
            r._1
          else {
            val L_PLAN_POWEROFF_CNT = add(r._1.L_PLAN_POWEROFF_CNT, r._2.L_PLAN_POWEROFF_CNT)
            val L_PROD_POWEROFF_DURATION = add(r._1.L_PROD_POWEROFF_DURATION, r._2.L_PROD_POWEROFF_DURATION)
            val L_TEMP_POWEROFF_DURATION = add(r._1.L_TEMP_POWEROFF_DURATION, r._2.L_TEMP_POWEROFF_DURATION)
            val L_TEMP_LOSS_POWER = add(r._1.L_TEMP_LOSS_POWER, r._2.L_TEMP_LOSS_POWER)
            val L_PLAN_POWEROFF_DURATION = add(r._1.L_PLAN_POWEROFF_DURATION, r._2.L_PLAN_POWEROFF_DURATION)
            val L_PLAN_LOSS_POWER = add(r._1.L_PLAN_LOSS_POWER, r._2.L_PLAN_LOSS_POWER)
            val L_PROD_LOSS_POWER = add(r._1.L_PROD_LOSS_POWER, r._2.L_PROD_LOSS_POWER)
            val L_PROD_POWEROFF_CNT = add(r._1.L_PROD_POWEROFF_CNT, r._2.L_PROD_POWEROFF_CNT)
            val L_TEMP_POWEROFF_CNT = add(r._1.L_TEMP_POWEROFF_CNT, r._2.L_TEMP_POWEROFF_CNT)
            val CONS_REPOWEROFF_PLAN_CNT = add(r._1.CONS_REPOWEROFF_PLAN_CNT, r._2.CONS_REPOWEROFF_PLAN_CNT)
            val CONS_REPOWEROFF_FAULT_CNT = add(r._1.CONS_REPOWEROFF_FAULT_CNT, r._2.CONS_REPOWEROFF_FAULT_CNT)
            val CONS_REPOWEROFF_TEMP_CNT = add(r._1.CONS_REPOWEROFF_TEMP_CNT, r._2.CONS_REPOWEROFF_TEMP_CNT)
            val CONS_REPOWEROFF_All_CNT = add(r._1.CONS_REPOWEROFF_All_CNT, r._2.CONS_REPOWEROFF_All_CNT)
            val DYJHTD_YHSL = add(r._1.DYJHTD_YHSL, r._2.DYJHTD_YHSL)
            val DYGZTD_YHSL = add(r._1.DYGZTD_YHSL, r._2.DYGZTD_YHSL)
            val DYLSJXTD_YHSL = add(r._1.DYLSJXTD_YHSL, r._2.DYLSJXTD_YHSL)
            
            AFixConsStatDRow(r._1.PI_ORG_NO, L_PLAN_POWEROFF_CNT, L_PROD_POWEROFF_DURATION, L_TEMP_POWEROFF_DURATION,
              L_TEMP_LOSS_POWER, L_PLAN_POWEROFF_DURATION, L_PLAN_LOSS_POWER, L_PROD_LOSS_POWER, L_PROD_POWEROFF_CNT,
              L_TEMP_POWEROFF_CNT, CONS_REPOWEROFF_PLAN_CNT, CONS_REPOWEROFF_FAULT_CNT, r._1.AREA_TYPE_CODE,
              CONS_REPOWEROFF_TEMP_CNT, CONS_REPOWEROFF_All_CNT, DYJHTD_YHSL, 
                           DYGZTD_YHSL, DYLSJXTD_YHSL)
          }
        }
    }
  }

  private def add2(ds1: Dataset[AFixLineStatDRow], ds2: Dataset[AFixLineStatDRow]) = {
    ds1.as("a").joinWith(ds2.as("b"), $"a.PI_ORG_NO" === $"b.PI_ORG_NO", "outer").map {
      r =>
        {
          if (r._1.PI_ORG_NO == null)
            r._2
          else if (r._2.PI_ORG_NO == null)
            r._1
          else {
            val LINESEG_PLAN_POWEROFF_CNT = add(r._1.LINESEG_PLAN_POWEROFF_CNT, r._2.LINESEG_PLAN_POWEROFF_CNT)
            val LINESEG_PROD_POWEROFF_DURATION = add(r._1.LINESEG_PROD_POWEROFF_DURATION, r._2.LINESEG_PROD_POWEROFF_DURATION)
            val LINESEG_TEMP_POWEROFF_DURATION = add(r._1.LINESEG_TEMP_POWEROFF_DURATION, r._2.LINESEG_TEMP_POWEROFF_DURATION)
            val LINESEG_PLAN_POWEROFF_DURATION = add(r._1.LINESEG_PLAN_POWEROFF_DURATION, r._2.LINESEG_PLAN_POWEROFF_DURATION)
            val LINESEG_PROD_POWEROFF_CNT = add(r._1.LINESEG_PROD_POWEROFF_CNT, r._2.LINESEG_PROD_POWEROFF_CNT)
            val LINESEG_TEMP_POWEROFF_CNT = add(r._1.LINESEG_TEMP_POWEROFF_CNT, r._2.LINESEG_TEMP_POWEROFF_CNT)
            val LINESEG_REPOWEROFF_PLAN_CNT = add(r._1.LINESEG_REPOWEROFF_PLAN_CNT, r._2.LINESEG_REPOWEROFF_PLAN_CNT)
            val LINESEG_REPOWEROFF_TEMP_CNT = add(r._1.LINESEG_REPOWEROFF_TEMP_CNT, r._2.LINESEG_REPOWEROFF_TEMP_CNT)
            val LINESEG_REPOWEROFF_FAULT_CNT = add(r._1.LINESEG_REPOWEROFF_FAULT_CNT, r._2.LINESEG_REPOWEROFF_FAULT_CNT)
            new AFixLineStatDRow(r._1.PI_ORG_NO, LINESEG_PLAN_POWEROFF_CNT, LINESEG_PROD_POWEROFF_DURATION,
              LINESEG_TEMP_POWEROFF_DURATION, LINESEG_PLAN_POWEROFF_DURATION,
              LINESEG_PROD_POWEROFF_CNT, LINESEG_TEMP_POWEROFF_CNT, LINESEG_REPOWEROFF_PLAN_CNT,
              LINESEG_REPOWEROFF_TEMP_CNT, LINESEG_REPOWEROFF_FAULT_CNT)
          }
        }
    }
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
        $"CONS_REPOWEROFF_FAULT_CNT", 
        
        $"AREA_TYPE_CODE"
        )
  }

}