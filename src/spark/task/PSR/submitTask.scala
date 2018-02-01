package spark.task.PSR

import java.util.Date
import spark.moudel.ATgDetailStatDRow
import spark.util.utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import spark.moudel.ALinesegTotalStatDProcessRow

object submitTask {
  def main(args: Array[String]) {
    org.apache.spark.sql.jdbc.unregisterOracleDialect.unregister
    args(0) match {
      case "ATMNL" => excute(args.tail)
      case _       => println("WRONG PARAMETER")
    }
  }

  def excute(args: Array[String]) {
    val name = "ATmnlOnoffStatD"
    val sparkConf = new SparkConf().setAppName(name)
      .set("spark.sql.shuffle.partitions", "8")
      //    .set("spark.yarn.executor.memoryOverhead","600")
      .set("spark.sql.autoBroadcastJoinThreshold", "0") //禁止自动广播
      .set("spark.shuffle.reduceLocality.enabled", "false")
    //      .set("spark.kryoserializer.buffer.max", "1024m")
    //    logConf.setLogLevels
    //    sparkConf.registerKryoClasses(Array(classOf[ATmnlOnoffStatDProcessRow], classOf[ATmnlOnoffStatDRow]))
    val sc = new SparkContext(sparkConf)
    val hc = new HiveContext(sc)
    import hc.implicits._

    args(0) match {
      case "batch" => batch(args(1), args(2), "oozie")
      case "oozie" => oozie
    }

    def batch(start: String, isInit: String, isBatch: String) {
      val startDate = utils.sdfDay.parse(start)
      val dataReader = new PSRDataReader(hc, startDate)
      val readerSet = sc.getPersistentRDDs.keySet
      //      val process = new ATmnlOnoffStatDProcess(hc, dataReader)
      //      val process1 = new ATgDetailStatDProcess(hc, dataReader)
      //      val process2 = new ALinesegDetailStatDProcess(hc, dataReader)
      //      val process3 = new ALineDetailStatDProcess(hc, dataReader)
      val process4 = new ATgTotalStatDProcess(hc, dataReader)
      val processCons = new AConsTotalStatDProcess(hc, dataReader)
      val process5 = new ALinesegTotalStatDProcess(hc, dataReader)
      //      val process6 = new ALineTotalStatDProcess(hc, dataReader)
      val process7 = new AFixPreStatD(hc, dataReader)
      val processBZ = new AFixPreStatD(hc, dataReader)
      val process8 = new AFixLineStatD(hc, dataReader)
      val statDate = dataReader.dateStr
      println(utils.sdfSec.format(new java.util.Date))
      println(statDate)

      // 省市县， 配变停电上电统计
      val aTgDetailStatD = dataReader.PWYW_PBTDSD_MX.persist
      val aTgTotalStatD = process4.execute(aTgDetailStatD).persist
      
      // 省市县， 用户停电上电统计
      val aConsDetailStatD = dataReader.PWYW_YHTDSD_MX.persist
      val aConsTotalStatD = processCons.execute(aConsDetailStatD).persist
      
      // 班组，配变停上电
      val aTgDetailStatD_BZ = dataReader.PWYW_PBTDSD_MX_BZ.persist
      val aTgTotalStatD_BZ = process4.execute(aTgDetailStatD_BZ).persist
      
      // 班组，用户停上电
      val aConsDetailStatD_BZ = dataReader.PWYW_YHTDSD_MX.persist
      val aConsTotalStatD_BZ = processCons.execute(aConsDetailStatD_BZ).persist

//      aTgTotalStatD.toDF.drop("TG_CAP").drop("PI_LINE_NO").drop("POWEROFF_CNT")
//        .toDF("PBID", "SSXL", "DWBM", "JHTDCS", "JHTDSC", "JHTDSSDL", "LSJXTDCS", "LSJXTDSC", "LSJXTDSSDL", "GZTDCS", "GZTDSC", "GZTDSSDL")
//        .join(dataReader.aTgTotalArchive, Seq("PBID"), "left_outer")
//        .printSchema

      process7.setData(aTgTotalStatD.toDF, aTgDetailStatD.toDF, aConsTotalStatD.toDF, aConsDetailStatD.toDF)
      processBZ.setData(aTgTotalStatD_BZ.toDF, aTgDetailStatD_BZ.toDF, aConsTotalStatD_BZ.toDF, aConsDetailStatD_BZ.toDF)
      val aFixPreOrg = process7.statProv.unionAll(process7.statCity).repartition(8).persist
      val aFixPreBr = process7.statTown.unionAll(processBZ.statBZ).repartition(8).persist
      
      process7.clearData
      

//      aFixPreOrg.toDF("DWBM", "ZYJHTDCS", "ZYGZTDSC", "ZYLSJXTDSC", "ZYLSJXYXDL", "ZYJHTDSC", "ZYJHTDYXDL", "ZYGZTDYXDL", "ZYGZTDCS",
//        "ZYLSJXTDCS", "TQCFTDJHCS", "TQCFDLGZCS", "CNW")
//        .join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer")
//        .printSchema()

      val aLinesegDetailStatD = dataReader.PWYW_KXTDSD_MX.persist
      val aLinesegTotalStatD = process5.execute(aLinesegDetailStatD).persist

//      aLinesegTotalStatD.toDF.drop("PI_LINE_NO").drop("POWEROFF_CNT").drop("POWEROFF_ALONE_CONS_CNT").drop("POWEROFF_NORMAL_CNT")
//        .toDF("XLID", "DWBM", "JHTDCS", "JHTDSC", "JHTDSSDL", "GZTDCS", "GZTDSC", "GZTDSSDL", "LSJXTDCS", "LSJXTDSC", "LSJXTDSSDL")
//        .join(dataReader.aLinesegTotalArchive, Seq("XLID"), "left_outer")
//        .printSchema()

      val aFixLineOrg = process8.statProv(aLinesegTotalStatD.toDF, aLinesegDetailStatD.toDF).unionAll(
        process8.statCity(aLinesegTotalStatD.toDF, aLinesegDetailStatD.toDF)).repartition(8).persist

//      aFixLineOrg.toDF("DWBM", "KXJHTDCS", "KXGZTDSC", "KXLSJHTDSC", "KXJHTDSC", "KXGZTDCS", "KXLSJHTDCS", "SLCFTDCSJH", "SLCFTDCSLSJH",
//        "SLCFTDCSGZ")
//        .join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer")
//        .printSchema()

      val aFixLineBr = process8.statTown(aLinesegTotalStatD.toDF, aLinesegDetailStatD.toDF).unionAll(
        process8.statBZ(aLinesegTotalStatD.toDF, aLinesegDetailStatD.toDF)).repartition(8).persist

      try {
        PSRToOracle.tgtotal(aTgTotalStatD.toDF.join(dataReader.aTgTotalArchive, Seq("TG_ID"), "left_outer"),
          "PWYW_PBTDSDTJ_R", dataReader.dateStr, true)
          
        PSRToOracle.consTotal(aConsTotalStatD.toDF.join(dataReader.aConsTotalArchive, Seq("CONS_NO","PI_ORG_NO"), "left_outer"),
          "PWYW_YHTDSDTJ_R", dataReader.dateStr, true)
          
        PSRToOracle.aFixPreStat(aFixPreOrg.toDF.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"),
          "PWYW_DWGDKKXTJ_SS_R", dataReader.dateStr, true)
        PSRToOracle.aFixPreStat(aFixPreBr.toDF.join(dataReader.aFixPreArchive, Seq("PI_ORG_NO"), "left_outer"),
          "PWYW_DWGDKKXTJ_BZ_R", dataReader.dateStr, true)
          
        PSRToOracle.linesegtotal(aLinesegTotalStatD.toDF.join(dataReader.aLinesegTotalArchive, Seq("LINE_SEG_NO"), "left_outer"),
          "PWYW_KXTDSDTJ_R", dataReader.dateStr, true)
        PSRToOracle.aFixLineStat(aFixLineOrg.toDF.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"),
          "PWYW_DWXLTDTJ_SS_R", dataReader.dateStr, true)
        PSRToOracle.aFixLineStat(aFixLineBr.toDF.join(dataReader.aFixLinesegArchive, Seq("PI_ORG_NO"), "left_outer"),
          "PWYW_DWXLTDTJ_BZ_R", dataReader.dateStr, true)
      }catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }

      val linePeriod = new LinePeriodStat(hc, dataReader, aConsTotalStatD.toDF, aTgTotalStatD.toDF.drop("PI_LINE_NO"),
        aLinesegTotalStatD.toDF, isInit, isBatch)
      try {
        linePeriod.statWeek
        linePeriod.statMonth
        linePeriod.statSeason
        linePeriod.statHY
        linePeriod.statYear
      } catch {
        case e: Exception => {
          e.printStackTrace()
          linePeriod.rollBack
        }
      } finally {
        linePeriod.clean
      }

      val aFixPeriod = new AFixPeriodStat(hc, dataReader, aFixPreOrg, aFixPreBr, aFixLineOrg, aFixLineBr, isInit, isBatch)
      try {
        aFixPeriod.statWeek
        aFixPeriod.statMonth
        aFixPeriod.statSeason
        aFixPeriod.statHY
        aFixPeriod.statYear
      } catch {
        case e: Exception => {
          e.printStackTrace()
          aFixPeriod.rollBack
        }
      } finally {
        aFixPeriod.clean
      }

      println(utils.sdfSec.format(new java.util.Date))
    }

    def oozie {
      val startDate = utils.sdfDay.format(new Date((new Date).getTime - 2 * 24 * 3600 * 1000l))
      batch(startDate, "ninit", "oozie")
    }
  }
}