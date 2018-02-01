package spark.task.PSR

import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import java.util.Calendar
import spark.util.utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class TZTJ(hc: HiveContext, val date: Date, dataReader: PSRDataReader, statType: String) extends Serializable {
  import hc.implicits._

  private val areaMap = Map[String, String]("2" -> "('0')", "3" -> "('1')")
  private var allArea: (String, (DataFrame, DataFrame, DataFrame)) = ("1", (null, null, null))
  private var areaMapData: Map[String, (DataFrame, DataFrame, DataFrame)] = null

  def setData(data: DataFrame, data1: DataFrame, data2: DataFrame): Unit = {
    areaMapData = areaMap.map {
      x =>
        {
          x._1 -> (data.filter(s"URAL_FLAG in ${x._2}").persist, data1.filter(s"URAL_FLAG in ${x._2}").persist, data2.filter(s"URAL_FLAG in ${x._2}").persist)
        }
    }
    allArea = (allArea._1, (data, data1, data2))
  }

  def clearData {
    areaMapData.foreach { x =>
      x._2._1.unpersist()
      x._2._2.unpersist()
      x._2._3.unpersist()
    }
  }

  def statProv = {
    val fun = (df: DataFrame) => {
      df.drop("WHBZ").withColumn("PI_ORG_NO", lit("232AF1D001B65527E055000000000001"))
    }

    statByArea(fun)
  }

  def statCity = {
    val CITY = (df: DataFrame) => {
      df.drop("WHBZ").join(dataReader.city, $"CITY" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").unionAll(
        df.drop("WHBZ").join(dataReader.townOfCity, $"TOWN" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").drop("TOWN")).unionAll(
          df.drop("WHBZ").join(dataReader.bzOfCity, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"CITY").drop("CITY").drop("BZ"))
    }

    statByArea(CITY)
  }

  def statTown = {
    val TOWN = (df: DataFrame) => {
      df.drop("WHBZ").join(dataReader.town, $"TOWN" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"TOWN").drop("TOWN").unionAll(
        df.drop("WHBZ").join(dataReader.bzOfTown, $"BZ" === $"PI_ORG_NO").withColumn("PI_ORG_NO", $"TOWN").drop("TOWN").drop("BZ"))
    }

    statByArea(TOWN)
  }

  def statBZ = {
    val BZ = (df: DataFrame) => {
      df.drop("PI_ORG_NO").withColumnRenamed("WHBZ", "PI_ORG_NO")
    }

    statByArea(BZ)
  }

  def statByArea(filterOrg: (DataFrame) => DataFrame) = {
    var data = stat(filterOrg(allArea._2._1), filterOrg(allArea._2._2), filterOrg(allArea._2._3)).withColumn("AREA_TYPE_CODE", lit(allArea._1))
    for (area <- areaMapData)
      data = data.unionAll(stat(filterOrg(area._2._1), filterOrg(area._2._2), filterOrg(allArea._2._3)).withColumn("AREA_TYPE_CODE", lit(area._1)))
    data
  }

  def stat(data: DataFrame, LJ: DataFrame, XL: DataFrame) = {
    val TZKGSL = data.filter("KGZT = '0' ").select("SBID", "PI_ORG_NO").distinct().groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "TZKGSL")

    val TZDATA = data.filter("KGZT = '0' ").select("SBID", "PI_ORG_NO", "XLXZ").persist

    val TZCS = TZDATA.groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "TZCS")

    val XLCS = XL.groupBy("PI_ORG_NO").agg(sum($"XLZCD") as "XLZCD")

    val BGL_TZCS = TZCS.join(XLCS, Seq("PI_ORG_NO")).withColumn("BGL_TZCS", expr("TZCS * 100 / XLZCD"))

    val LJ_TZKGSL = LJ.groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "LJ_TZKGSL")

    val ZX_TZCS = TZDATA.filter("XLXZ = 1").groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "ZX_TZCS")
    val ZHIX_TZCS = TZDATA.filter("XLXZ = 2").groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "ZHIX_TZCS")
    val FZX_TZCS = TZDATA.filter("XLXZ = 3").groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "FZX_TZCS")
    val FDX_TZCS = TZDATA.filter("XLXZ = 4").groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "FDX_TZCS")
    val KX_TZCS = TZDATA.filter("XLXZ = 5").groupBy("PI_ORG_NO").count.toDF("PI_ORG_NO", "KX_TZCS")

    TZKGSL.join(TZCS, Seq("PI_ORG_NO")).join(BGL_TZCS, Seq("PI_ORG_NO")).join(LJ_TZKGSL, Seq("PI_ORG_NO")).join(ZX_TZCS, Seq("PI_ORG_NO"))
      .join(ZHIX_TZCS, Seq("PI_ORG_NO")).join(FZX_TZCS, Seq("PI_ORG_NO")).join(FDX_TZCS, Seq("PI_ORG_NO")).join(KX_TZCS, Seq("PI_ORG_NO"))
  }

  def setData: Unit = {

    val calendar = Calendar.getInstance
    val date2 = utils.sdfDay.format(date)

    val date1 = statType match {
      case "DAY" => {
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, -1)
        utils.sdfDay.format(calendar.getTime)
      }
      case "WEEK" => {
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, -7)
        utils.sdfDay.format(calendar.getTime)
      }
      case "MONTH" => {
        calendar.setTime(date)
        calendar.add(Calendar.MONTH, -1)
        utils.sdfDay.format(calendar.getTime)
      }
      case "SEASON" => {
        calendar.setTime(date)
        calendar.add(Calendar.MONTH, -3)
        utils.sdfDay.format(calendar.getTime)
      }
      case "HY" => {
        calendar.setTime(date)
        calendar.add(Calendar.MONTH, -6)
        utils.sdfDay.format(calendar.getTime)
      }
      case "YEAR" => {
        calendar.setTime(date)
        calendar.add(Calendar.YEAR, -1)
        utils.sdfDay.format(calendar.getTime)
      }
    }

    val year = date1.take(4)
    val lastYear = {
      calendar.setTime(utils.sdfDay.parse(date1))
      calendar.add(Calendar.YEAR, -1)
      utils.sdfYear.format(calendar.getTime)
    }

    val TJRQ = statType match {
      case "DAY"   => date1
      case "WEEK"  => date1
      case "MONTH" => date1.dropRight(2)
      case "SEASON" => date1.substring(4, 6) match {
        case "01" => year + "01" case "04" => year + "02" case "07" => year + "03" case "10" => year + "04"
      }
      case "HY" => date1.substring(4, 6) match {
        case "01" => year + "01" case "07" => year + "02"
      }
      case "YEAR" => year
    }

    val TJRQ_HB = statType match {
      case "DAY" =>
        val date = utils.sdfDay.parse(date1)
        utils.sdfDay.format(new Date(date.getTime - 24 * 3600 * 1000l))
      case "WEEK" =>
        val date = utils.sdfDay.parse(date1)
        utils.sdfDay.format(new Date(date.getTime - 7 * 24 * 3600 * 1000l))
      case "MONTH" =>
        calendar.setTime(utils.sdfDay.parse(date1))
        calendar.add(Calendar.MONTH, -1)
        utils.sdfMonth.format(calendar.getTime)
      case "SEASON" =>
        calendar.setTime(utils.sdfDay.parse(date1))
        calendar.add(Calendar.MONTH, -3)
        val dateStr = utils.sdfMonth.format(calendar.getTime)
        dateStr.substring(4, 6) match {
          case "01" => year + "01" case "04" => year + "02" case "07" => year + "03" case "10" => year + "04"
        }
      case "HY" =>
        calendar.setTime(utils.sdfDay.parse(date1))
        calendar.add(Calendar.MONTH, -6)
        val dateStr = utils.sdfMonth.format(calendar.getTime)
        dateStr.substring(4, 6) match {
          case "01" => year + "01" case "07" => year + "02"
        }
    }

    val TJRQ_TB = statType match {
      case "DAY" =>
        lastYear + TJRQ.takeRight(4)
      case "WEEK" =>
        lastYear + TJRQ.takeRight(4)
      case "MONTH" =>
        lastYear + TJRQ.takeRight(2)
      case "SEASON" =>
        lastYear + TJRQ.takeRight(2)
      case "HY" =>
        lastYear + TJRQ.takeRight(2)
      case "YEAR" =>
        lastYear
    }

    val ST_PMS_KG_DEV = hc.read.jdbc(utils.url113, "(SELECT OBJ_ID as SBID, DWID as PI_ORG_NO, WHBZ, SFNW as URAL_FLAG, SSXL as XLID FROM PWYW.ST_PMS_KG_DEV", utils.cp)
      .join(dataReader.T_SB_ZWYC_XL.select("XLID", "XLXZF"), Seq("XLID")).drop("XLID").persist

    val PWYW_T_DMS_SWITCH_STATE = hc.read.jdbc(utils.url113, s"""(SELECT SBID, KGZT FROM PWYW.PWYW_T_DMS_SWITCH_STATE WHERE KGDZSJ >= to_date('$date1','yyyymmdd') 
      AND KGDZSJ < to_date('$date2','yyyymmdd') """, utils.cp).join(ST_PMS_KG_DEV, Seq("SBID"))

    var LJ_TZKGSL: DataFrame = null

    if (statType == "DAY") {
      calendar.setTime(date)
      if (calendar.get(Calendar.DAY_OF_YEAR) == 1)
        PWYW_T_DMS_SWITCH_STATE.select("SBID", "KGZT", "PI_ORG_NO", "URAL_FLAG").distinct().write.mode("overwrite").saveAsTable("PWYW.LJ_TZKGSL")
      else {
        LJ_TZKGSL = PWYW_T_DMS_SWITCH_STATE.select("SBID", "KGZT", "PI_ORG_NO", "URAL_FLAG").distinct().unionAll(hc.table("PWYW.LJ_TZKGSL")).distinct().persist
        LJ_TZKGSL.write.mode("overwrite").saveAsTable("PWYW.TMPKG")
        hc.table("PWYW.TMPKG").write.mode("overwrite").saveAsTable("PWYW.LJ_TZKGSL")
      }
    }

    val XLCD = dataReader.T_SB_ZWYC_XL.select($"WHBZ" as "PMS_DWID", $"XLZCD").join(dataReader.ST_PMS_YX_DW.select($"PMS_DWID", $"SJDWID" as "PI_ORG_NO"),
      Seq("PMS_DWID"))
      .drop("PMS_DWID")

    setData(PWYW_T_DMS_SWITCH_STATE, LJ_TZKGSL, XLCD)

  }

}