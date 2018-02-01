package spark.task.PSR

import spark.util.NullableStatProcess
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.Dataset
import java.util.Calendar
import spark.util.utils
import spark.moudel.ALinesegTotalStatDStatRateRow
import spark.moudel.ATgTotalStatRow
import spark.moudel.AConsTotalStatDRow

class LinePeriodStat(hc: HiveContext, dataReader: PSRDataReader, aConsTotalStatD : DataFrame, aTgTotalStatD: DataFrame, aLinesegTotalStatDStatRate: DataFrame, isInit: String, isBatch: String) extends Serializable with NullableStatProcess {
  import hc.implicits._

  private val date = dataReader.date
  private val init = if (isInit == "init") true else false
  private val isWrite = if (isBatch == "batch") false else true
  private val calendar = Calendar.getInstance
  calendar.setTime(date)

  private val tableArray = Array("PWYW.PWYW_PBTDSDTJ_Z", "PWYW.PWYW_KXTDSDTJ_Z", "PWYW.PWYW_YHTDSDTJ_Z",
    "PWYW.PWYW_PBTDTJ_Y", "PWYW.PWYW_KXTDSDTJ_Y", "PWYW.PWYW_YHTDSDTJ_Y",
    "PWYW.PWYW_PBTDSDTJ_J", "PWYW.PWYW_KXTDSDTJ_J", "PWYW.PWYW_YHTDSDTJ_J",
    "PWYW.PWYW_PBTDSDTJ_BN", "PWYW.PWYW_KXTDSDTJ_BN", "PWYW.PWYW_YHTDSDTJ_BN",
    "PWYW.PWYW_PBTDSDTJ_N", "PWYW.PWYW_KXTDSDTJ_N", "PWYW.PWYW_YHTDSDTJ_N")

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
    println("line rollBack")
    if(!init) tableRBTMP.foreach(r => hc.table(r).write.mode("overwrite").saveAsTable(r.dropRight(6)))
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
      aTgTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_Z")
      if (ifWrite) PSRToOracle.tgtotalW(aTgTotalStatD.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_Z", STAT_DATE_S, STAT_DATE_E)
          
      aLinesegTotalStatDStatRate.write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_Z")
      if (ifWrite) PSRToOracle.linesegtotalW(aLinesegTotalStatDStatRate.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_Z", STAT_DATE_S, STAT_DATE_E)
      
      aConsTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_Z")
      if (ifWrite) PSRToOracle.constotalW(aConsTotalStatD.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_Z", STAT_DATE_S, STAT_DATE_E)
    } else {
      val df1 = add1(hc.table("PWYW.PWYW_PBTDSDTJ_Z").as[ATgTotalStatRow], aTgTotalStatD.as[ATgTotalStatRow]).toDF.persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_Z")
      if (ifWrite) PSRToOracle.tgtotalW(df1.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_Z", STAT_DATE_S, STAT_DATE_E)
      
      val df2 = add2(hc.table("PWYW.PWYW_KXTDSDTJ_Z").as[ALinesegTotalStatDStatRateRow], aLinesegTotalStatDStatRate.as[ALinesegTotalStatDStatRateRow]).toDF.persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_Z")
      if (ifWrite) PSRToOracle.linesegtotalW(df2.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_Z", STAT_DATE_S, STAT_DATE_E)
      
      val df3 = add3(hc.table("PWYW.PWYW_YHTDSDTJ_Z").as[AConsTotalStatDRow], aConsTotalStatD.as[AConsTotalStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_Z")
      
      if (ifWrite) PSRToOracle.constotalW(df3.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_Z", STAT_DATE_S, STAT_DATE_E)
      
      df1.unpersist(); df2.unpersist();
      df3.unpersist();
      
    }
  }

  def statMonth {
    val STAT_DATE = utils.sdfMonth.format(date)

    val dom = calendar.get(Calendar.DAY_OF_MONTH)
    val maxDom = calendar.getActualMaximum(Calendar.DAY_OF_MONTH)
    val ifWrite = isWrite || dom == maxDom

    if (init || dom == 1) {
      aTgTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_Y")
      if (ifWrite) PSRToOracle.tgtotal(aTgTotalStatD.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_Y", STAT_DATE, false)
      aLinesegTotalStatDStatRate.write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_Y")
      if (ifWrite) PSRToOracle.linesegtotal(aLinesegTotalStatDStatRate.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_Y", STAT_DATE, false)
      aConsTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_Y")
      if (ifWrite) PSRToOracle.consTotal(aConsTotalStatD.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_Y", STAT_DATE, false)
    
    } else {
      val df1 = add1(hc.table("PWYW.PWYW_PBTDSDTJ_Y").as[ATgTotalStatRow], aTgTotalStatD.as[ATgTotalStatRow]).toDF.persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_Y")
      if (ifWrite) PSRToOracle.tgtotal(df1.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_Y", STAT_DATE, false)
      val df2 = add2(hc.table("PWYW.PWYW_KXTDSDTJ_Y").as[ALinesegTotalStatDStatRateRow], aLinesegTotalStatDStatRate.as[ALinesegTotalStatDStatRateRow]).toDF.persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_Y")
      if (ifWrite) PSRToOracle.linesegtotal(df2.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_Y", STAT_DATE, false)
      
      val df3 = add3(hc.table("PWYW.PWYW_YHTDSDTJ_Y").as[AConsTotalStatDRow], aConsTotalStatD.as[AConsTotalStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_Y")
      if (ifWrite) PSRToOracle.consTotal(df3.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_Y", STAT_DATE, false)
      
      df1.unpersist(); df2.unpersist();df3.unpersist();
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
      aTgTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_J")
      if (ifWrite) PSRToOracle.tgtotal(aTgTotalStatD.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_J", STAT_DATE, false)
      aLinesegTotalStatDStatRate.write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_J")
      if (ifWrite) PSRToOracle.linesegtotal(aLinesegTotalStatDStatRate.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_J", STAT_DATE, false)
      aConsTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_J")
      if (ifWrite) PSRToOracle.consTotal(aConsTotalStatD.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_J", STAT_DATE, false)
    
    } else {
      val df1 = add1(hc.table("PWYW.PWYW_PBTDSDTJ_J").as[ATgTotalStatRow], aTgTotalStatD.as[ATgTotalStatRow]).toDF.persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_J")
      if (ifWrite) PSRToOracle.tgtotal(df1.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_J", STAT_DATE, false)
      val df2 = add2(hc.table("PWYW.PWYW_KXTDSDTJ_J").as[ALinesegTotalStatDStatRateRow], aLinesegTotalStatDStatRate.as[ALinesegTotalStatDStatRateRow]).toDF.persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_J")
      if (ifWrite) PSRToOracle.linesegtotal(df2.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_J", STAT_DATE, false)
      
      val df3 = add3(hc.table("PWYW.PWYW_YHTDSDTJ_J").as[AConsTotalStatDRow], aConsTotalStatD.as[AConsTotalStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_J")
      if (ifWrite) PSRToOracle.consTotal(df3.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_J", STAT_DATE, false)
      
      df1.unpersist(); df2.unpersist();df3.unpersist();
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
      aTgTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_BN")
      if (ifWrite) PSRToOracle.tgtotal(aTgTotalStatD.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_BN", STAT_DATE, false)
      aLinesegTotalStatDStatRate.write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_BN")
      if (ifWrite) PSRToOracle.linesegtotal(aLinesegTotalStatDStatRate.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_BN", STAT_DATE, false)
      aConsTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_BN")
      if (ifWrite) PSRToOracle.consTotal(aConsTotalStatD.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_BN", STAT_DATE, false)
    
    } else {
      val df1 = add1(hc.table("PWYW.PWYW_PBTDSDTJ_BN").as[ATgTotalStatRow], aTgTotalStatD.as[ATgTotalStatRow]).toDF.persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_BN")
      if (ifWrite) PSRToOracle.tgtotal(df1.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_BN", STAT_DATE, false)
      val df2 = add2(hc.table("PWYW.PWYW_KXTDSDTJ_BN").as[ALinesegTotalStatDStatRateRow], aLinesegTotalStatDStatRate.as[ALinesegTotalStatDStatRateRow]).toDF.persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_BN")
      if (ifWrite) PSRToOracle.linesegtotal(df2.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_BN", STAT_DATE, false)
      
      val df3 = add3(hc.table("PWYW.PWYW_YHTDSDTJ_BN").as[AConsTotalStatDRow], aConsTotalStatD.as[AConsTotalStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_BN")
      if (ifWrite) PSRToOracle.consTotal(df3.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_BN", STAT_DATE, false)
      
      df1.unpersist(); df2.unpersist();df3.unpersist();
    }
  }

  def statYear {
    val STAT_DATE = utils.sdfYear.format(date)

    val doy = calendar.get(Calendar.DAY_OF_YEAR)
    val maxDoy = calendar.getActualMaximum(Calendar.DAY_OF_YEAR)
    val ifWrite = isWrite || doy == maxDoy

    if (init || doy == 1) {
      aTgTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_N")
      if (ifWrite) PSRToOracle.tgtotal(aTgTotalStatD.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_N", STAT_DATE, false)
      aLinesegTotalStatDStatRate.write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_N")
      if (ifWrite) PSRToOracle.linesegtotal(aLinesegTotalStatDStatRate.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_N", STAT_DATE, false)
      aConsTotalStatD.write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_N")
      if (ifWrite) PSRToOracle.consTotal(aConsTotalStatD.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_N", STAT_DATE, false)
    
    } else {
      val df1 = add1(hc.table("PWYW.PWYW_PBTDSDTJ_N").as[ATgTotalStatRow], aTgTotalStatD.as[ATgTotalStatRow]).toDF.persist
      df1.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_PBTDSDTJ_N")
      if (ifWrite) PSRToOracle.tgtotal(df1.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"), "PWYW_PBTDSDTJ_N", STAT_DATE, false)
      val df2 = add2(hc.table("PWYW.PWYW_KXTDSDTJ_N").as[ALinesegTotalStatDStatRateRow], aLinesegTotalStatDStatRate.as[ALinesegTotalStatDStatRateRow]).toDF.persist
      df2.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_KXTDSDTJ_N")
      if (ifWrite) PSRToOracle.linesegtotal(df2.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"), "PWYW_KXTDSDTJ_N", STAT_DATE, false)
      
      val df3 = add3(hc.table("PWYW.PWYW_YHTDSDTJ_N").as[AConsTotalStatDRow], aConsTotalStatD.as[AConsTotalStatDRow]).toDF.persist
      df3.write.mode("overwrite").saveAsTable("PWYW.TMP")
      hc.table("PWYW.TMP").write.mode("overwrite").saveAsTable("PWYW.PWYW_YHTDSDTJ_N")
      if (ifWrite) PSRToOracle.consTotal(df3.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"), "PWYW_YHTDSDTJ_N", STAT_DATE, false)
      
      df1.unpersist(); df2.unpersist();df3.unpersist();
    }
  }

  private def add1(ds1: Dataset[ATgTotalStatRow], ds2: Dataset[ATgTotalStatRow]) = {
    ds1.as("a").joinWith(ds2.as("b"), $"a.TG_ID" === $"b.TG_ID", "outer").map {
      r =>
        {
          if (r._1.TG_ID == null)
            r._2
          else if (r._2.TG_ID == null)
            r._1
          else {
            val POWEROFF_CNT = add(r._1.POWEROFF_CNT, r._2.POWEROFF_CNT)
            val PLAN_POWEROFF_CNT = add(r._1.PLAN_POWEROFF_CNT, r._2.PLAN_POWEROFF_CNT)
            val PLAN_POWEROFF_DURATION = add(r._1.PLAN_POWEROFF_DURATION, r._2.PLAN_POWEROFF_DURATION)
            val PLAN_LOSS_POWER = add(r._1.PLAN_LOSS_POWER, r._2.PLAN_LOSS_POWER)
            val TEMP_POWEROFF_CNT = add(r._1.TEMP_POWEROFF_CNT, r._2.TEMP_POWEROFF_CNT)
            val TEMP_POWEROFF_DURATION = add(r._1.TEMP_POWEROFF_DURATION, r._2.TEMP_POWEROFF_DURATION)
            val TEMP_LOSS_POWER = add(r._1.TEMP_LOSS_POWER, r._2.TEMP_LOSS_POWER)
            val PROD_POWEROFF_CNT = add(r._1.PROD_POWEROFF_CNT, r._2.PROD_POWEROFF_CNT)
            val PROD_POWEROFF_DURATION = add(r._1.PROD_POWEROFF_DURATION, r._2.PROD_POWEROFF_DURATION)
            val PROD_LOSS_POWER = add(r._1.PROD_LOSS_POWER, r._2.PROD_LOSS_POWER)
            ATgTotalStatRow(r._1.TG_ID, r._1.LINE_SEG_NO, r._1.PI_ORG_NO, r._1.TG_CAP, POWEROFF_CNT, PLAN_POWEROFF_CNT, PLAN_POWEROFF_DURATION,
              PLAN_LOSS_POWER, TEMP_POWEROFF_CNT, TEMP_POWEROFF_DURATION, TEMP_LOSS_POWER, PROD_POWEROFF_CNT, PROD_POWEROFF_DURATION, PROD_LOSS_POWER)
          }
        }
    }
  }

  private def add2(ds1: Dataset[ALinesegTotalStatDStatRateRow], ds2: Dataset[ALinesegTotalStatDStatRateRow]) = {
    ds1.as("a").joinWith(ds2.as("b"), $"a.PI_ORG_NO" === $"b.PI_ORG_NO" and $"a.LINE_SEG_NO" === $"b.LINE_SEG_NO", "outer").map {
      r =>
        {
          if (r._1.LINE_SEG_NO == null)
            r._2
          else if (r._2.LINE_SEG_NO == null)
            r._1
          else {
            val POWEROFF_CNT = add(r._1.POWEROFF_CNT, r._2.POWEROFF_CNT)
            val POWEROFF_ALONE_CONS_CNT = add(r._1.POWEROFF_ALONE_CONS_CNT, r._2.POWEROFF_ALONE_CONS_CNT)
            val POWEROFF_NORMAL_CNT = add(r._1.POWEROFF_NORMAL_CNT, r._2.POWEROFF_NORMAL_CNT)
            val PLAN_POWEROFF_CNT = add(r._1.PLAN_POWEROFF_CNT, r._2.PLAN_POWEROFF_CNT)
            val PLAN_POWEROFF_DURATION = add(r._1.PLAN_POWEROFF_DURATION, r._2.PLAN_POWEROFF_DURATION)
            val PLAN_LOSS_POWER = add(r._1.PLAN_LOSS_POWER, r._2.PLAN_LOSS_POWER)
            val TEMP_POWEROFF_CNT = add(r._1.TEMP_POWEROFF_CNT, r._2.TEMP_POWEROFF_CNT)
            val TEMP_POWEROFF_DURATION = add(r._1.TEMP_POWEROFF_DURATION, r._2.TEMP_POWEROFF_DURATION)
            val TEMP_LOSS_POWER = add(r._1.TEMP_LOSS_POWER, r._2.TEMP_LOSS_POWER)
            val PROD_POWEROFF_CNT = add(r._1.PROD_POWEROFF_CNT, r._2.PROD_POWEROFF_CNT)
            val PROD_POWEROFF_DURATION = add(r._1.PROD_POWEROFF_DURATION, r._2.PROD_POWEROFF_DURATION)
            val PROD_LOSS_POWER = add(r._1.PROD_LOSS_POWER, r._2.PROD_LOSS_POWER)
            new ALinesegTotalStatDStatRateRow(r._1.LINE_SEG_NO, r._1.PI_ORG_NO, r._1.PI_LINE_NO, POWEROFF_CNT, POWEROFF_ALONE_CONS_CNT, POWEROFF_NORMAL_CNT,
              PLAN_POWEROFF_CNT, PLAN_POWEROFF_DURATION, PLAN_LOSS_POWER, TEMP_POWEROFF_CNT, TEMP_POWEROFF_DURATION, TEMP_LOSS_POWER, PROD_POWEROFF_CNT,
              PROD_POWEROFF_DURATION, PROD_LOSS_POWER)
          }
        }
    }
  }
  
   private def add3(ds1: Dataset[AConsTotalStatDRow], ds2: Dataset[AConsTotalStatDRow]) = {
    ds1.as("a").joinWith(ds2.as("b"), $"a.CONS_NO" === $"b.CONS_NO", "outer").map {
      r =>
        {
          if (r._1.CONS_NO == null)
            r._2
          else if (r._2.CONS_NO == null)
            r._1
          else {
            val POWEROFF_CNT = add(r._1.POWEROFF_CNT, r._2.POWEROFF_CNT)
            val PLAN_POWEROFF_CNT = add(r._1.PLAN_POWEROFF_CNT, r._2.PLAN_POWEROFF_CNT)
            val PLAN_POWEROFF_DURATION = add(r._1.PLAN_POWEROFF_DURATION, r._2.PLAN_POWEROFF_DURATION)
            val PLAN_LOSS_POWER = add(r._1.PLAN_LOSS_POWER, r._2.PLAN_LOSS_POWER)
            val TEMP_POWEROFF_CNT = add(r._1.TEMP_POWEROFF_CNT, r._2.TEMP_POWEROFF_CNT)
            val TEMP_POWEROFF_DURATION = add(r._1.TEMP_POWEROFF_DURATION, r._2.TEMP_POWEROFF_DURATION)
            val TEMP_LOSS_POWER = add(r._1.TEMP_LOSS_POWER, r._2.TEMP_LOSS_POWER)
            val PROD_POWEROFF_CNT = add(r._1.PROD_POWEROFF_CNT, r._2.PROD_POWEROFF_CNT)
            val PROD_POWEROFF_DURATION = add(r._1.PROD_POWEROFF_DURATION, r._2.PROD_POWEROFF_DURATION)
            val PROD_LOSS_POWER = add(r._1.PROD_LOSS_POWER, r._2.PROD_LOSS_POWER)
            new AConsTotalStatDRow(r._1.CONS_NO, POWEROFF_CNT,r._1.PI_ORG_NO,
              PLAN_POWEROFF_CNT, PLAN_POWEROFF_DURATION, PLAN_LOSS_POWER, 
              TEMP_POWEROFF_CNT, TEMP_POWEROFF_DURATION, TEMP_LOSS_POWER,
              PROD_POWEROFF_CNT,PROD_POWEROFF_DURATION, PROD_LOSS_POWER)
          }
        }
    }
  }

}