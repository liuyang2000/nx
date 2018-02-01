package spark.task.PSR

import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.DataTypes
import java.sql.Types
import org.apache.spark.sql.types.DataType
import spark.util.utils
import org.apache.spark.sql.DataFrame
import spark.moudel.ATgDetailStatDProcessRow
import org.apache.spark.sql.functions._
import spark.moudel.ATgDetailStatDRow
import spark.moudel.ALinesegDetailStatDRow
import spark.moudel.AConsDetailStatDRow

class PSRDataReader(hc: HiveContext, val date: Date) extends Serializable {

  def incDate {
    date.setTime(date.getTime + 24 * 3600 * 1000)
  }

  def dateStr = utils.sdfDay.format(date)

  //  def buildArray(date: Date) = {
  //    val date1 = utils.sdfDay.format(date)
  //    val date2 = utils.sdfDay.format(new Date(date.getTime + 24 * 3600 * 1000))
  //
  //    for (i <- utils.partBArray)
  //      yield i + s" and POWEROFF_TIME > to_date('$date1','yyyymmdd') and POWEROFF_TIME < to_date('$date2','yyyymmdd')"
  //  }

  import hc.implicits._

  val ST_TQ_BYQ = hc.sql("SELECT * FROM PWYW_ARCH.ST_TQ_BYQ").persist()

  val PB_CAP = ST_TQ_BYQ.select($"PMS_BYQ_BS" as "TG_ID", $"TG_CAP")
  
  /**
   * 1 prod
   * 2 plan
   * 3 temp
   */
  def POWER_OFF_TYPE(PROD_POWERONOFF_FLAG: String, PLAN_POWERONOFF_FLAG: String): java.lang.Integer =
    if (PROD_POWERONOFF_FLAG == "2") 1 
      else if (PROD_POWERONOFF_FLAG == "1" && PLAN_POWERONOFF_FLAG == "1")  2
      else if (PROD_POWERONOFF_FLAG == "1" && PLAN_POWERONOFF_FLAG == "2")  3
      else 0

  val POWER_OFF_TYPEUDF = udf(POWER_OFF_TYPE _)
  
  val PWYW_YHTDSD_MX = hc.sql(s"SELECT * FROM PWYW.PWYW_YHTDSD_MX WHERE dt = $dateStr")
      .join(hc.sql(s"SELECT PBID,TDLB,JHLB,TDSJ,SDSJ FROM PWYW.E_PB_POWER_ONOFF WHERE dt = $dateStr"),Seq("PBID","TDSJ","SDSJ"),"inner").repartition(4)
      .select($"CONS_NO",$"CONS_NAME",$"ELEC_ADDR",$"DWBM".as("PI_ORG_NO"),POWER_OFF_TYPEUDF($"TDLB", $"JHLB") as "POWER_OFF_TYPE", $"JHLB" as "PLAN_OFF_TYPE",
      $"TDSC".as("POWEROFF_TOTAL_TIME"),lit(10).as("LOSS_POWER"), $"TDSJ".as("POWEROFF_TIME"), $"SDSJ".as("POWERON_TIME"))
      .as[AConsDetailStatDRow]
  
  val PWYW_YHTDSD_MX_BZ = hc.sql(s"SELECT * FROM PWYW.PWYW_YHTDSD_MX WHERE dt = $dateStr")
      .join(hc.sql(s"SELECT PBID,TDLB,JHLB,TDSJ,SDSJ FROM PWYW.E_PB_POWER_ONOFF WHERE dt = $dateStr"),Seq("PBID","TDSJ","SDSJ"),"inner").repartition(4)
      .select($"CONS_NO",$"CONS_NAME",$"ELEC_ADDR",$"BZID".as("PI_ORG_NO"),POWER_OFF_TYPEUDF($"TDLB", $"JHLB") as "POWER_OFF_TYPE", $"JHLB" as "PLAN_OFF_TYPE",
      $"TDSC".as("POWEROFF_TOTAL_TIME"),lit(10).as("LOSS_POWER"), $"TDSJ".as("POWEROFF_TIME"), $"SDSJ".as("POWERON_TIME"))
      .as[AConsDetailStatDRow]

  val PWYW_PBTDSD_MX = hc.sql(s"SELECT * FROM PWYW.E_PB_POWER_ONOFF WHERE dt = $dateStr").repartition(8)
    .join(PB_CAP, $"TG_ID" === $"PBID", "left_outer").drop("TG_ID")
    .select($"TDSDBS" as "ONOFF_FLAG", $"TDSDYXBZ" as "AVAI_FLAG", $"PBID" as "TG_ID", lit(1) as "POWEROFF_TIME", lit(1) as "POWERON_TIME",
      $"SSXL" as "LINE_SEG_NO", $"TG_CAP", lit(null) as "PI_LINE_NO", $"DWBM" as "PI_ORG_NO", $"TDSC" as "POWEROFF_TOTAL_TIME",
      expr("NVL(TDSC * TG_CAP * 0.8, 0)") cast "float" as "LOSS_POWER", 
      POWER_OFF_TYPEUDF($"TDLB", $"JHLB") as "POWER_OFF_TYPE", $"JHLB" as "PLAN_OFF_TYPE")
    .filter("AVAI_FLAG = '1' and ONOFF_FLAG = '0' ").as[ATgDetailStatDRow]
  
  val PWYW_PBTDSD_MX_BZ = hc.sql(s"SELECT * FROM PWYW.E_PB_POWER_ONOFF WHERE dt = $dateStr").repartition(8)
    .join(PB_CAP, $"TG_ID" === $"PBID", "left_outer").drop("TG_ID")
    .select($"TDSDBS" as "ONOFF_FLAG", $"TDSDYXBZ" as "AVAI_FLAG", $"PBID" as "TG_ID", lit(1) as "POWEROFF_TIME", lit(1) as "POWERON_TIME",
      $"SSXL" as "LINE_SEG_NO", $"TG_CAP", lit(null) as "PI_LINE_NO", $"BZID" as "PI_ORG_NO", $"TDSC" as "POWEROFF_TOTAL_TIME",
      expr("NVL(TDSC * TG_CAP * 0.8, 0)") cast "float" as "LOSS_POWER", POWER_OFF_TYPEUDF($"TDLB", $"JHLB") as "POWER_OFF_TYPE", $"JHLB" as "PLAN_OFF_TYPE")
    .filter("AVAI_FLAG = '1' and ONOFF_FLAG = '0' ").as[ATgDetailStatDRow]

  val PWYW_KXTDSD_MX = hc.sql(s"SELECT * FROM PWYW.PWYW_KXTDSD_MX WHERE dt = $dateStr").repartition(8)
    .select($"TDSDBS" as "ONOFF_FLAG", $"TDSDYXBZ" as "AVAI_FLAG", lit(1) as "POWEROFF_TIME", lit(1) as "POWERON_TIME", $"XLID" as "LINE_SEG_NO",
      lit(null) as "PI_LINE_NO", $"DWBM" as "PI_ORG_NO", $"TDCS" as "POWEROFF_TOTAL_TIME", lit(0f) as "LOSS_POWER",
      POWER_OFF_TYPEUDF($"TDLB", $"JHLB") as "POWER_OFF_TYPE", $"JHLB" as "PLAN_OFF_TYPE")
    .filter("AVAI_FLAG = '1' and ONOFF_FLAG = '0' ").as[ALinesegDetailStatDRow]

  val ST_PMS_YX_DW = hc.sql("SELECT * FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

  val orgRela = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"SJDWID" as "CITY", $"PMS_DWID" as "TOWN")
    .join(ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"SJDWID" as "TOWN", $"PMS_DWID" as "BZ"), Seq("TOWN")).persist

  val city = ST_PMS_YX_DW.filter("PMS_DWCJ = '4' ").select($"PMS_DWID" as "CITY").distinct.persist

  val town = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"PMS_DWID" as "TOWN").distinct.persist

  val bz = ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"PMS_DWID" as "BZ").distinct.persist

  val townOfCity = orgRela.select("CITY", "TOWN").distinct.persist

  val bzOfCity = orgRela.select("CITY", "BZ").distinct.persist

  val bzOfTown = orgRela.select("TOWN", "BZ").distinct.persist
  
  
  val CONSDET = hc.sql("""SELECT CC.CONS_NO,CC.CONS_NAME,CC.ELEC_ADDR,GTG.YX_TQ_BS,GTG.YX_TG_MC, GTG.PMS_BYQ_BS,GTG.PMS_BYQ_MC ,
    GTG.DQTZ,GTG.BZID,GTG.CNW,GTG.PMS_DWBM,GTG.PMS_DWMC,GTG.PMS_DWCJ,DW.SJDWID, DW.SJDWMC
    FROM PWYW_ARCH.C_MP_DET CMP, PWYW_ARCH.C_CONS_DET CC , PWYW_ARCH.ST_TQ_BYQ GTG ,PWYW_ARCH.ST_PMS_YX_DW DW
    WHERE CMP.TG_ID = GTG.YX_TQ_BS AND CMP.CONS_ID = CC.CONS_ID AND GTG.PMS_DWBM = DW.PMS_DWID""")
    
  val aConsTotalArchive = CONSDET.select($"CONS_NO",$"CONS_NAME",$"ELEC_ADDR",$"YX_TQ_BS",$"YX_TG_MC",$"PMS_BYQ_BS",$"PMS_BYQ_MC",
    $"DQTZ",$"BZID",$"CNW",$"PMS_DWBM" as "PI_ORG_NO",$"PMS_DWMC",$"PMS_DWCJ",$"SJDWID",$"SJDWMC")

  val TQDYYHS = hc.sql("""SELECT CMP.CONS_ID, GTG.PMS_BYQ_BS as PBID FROM PWYW_ARCH.C_MP_DET CMP, PWYW_ARCH.C_CONS_DET CC , PWYW_ARCH.ST_TQ_BYQ GTG 
    WHERE CMP.TG_ID = GTG.YX_TQ_BS AND CMP.CONS_ID = CC.CONS_ID""").groupBy("PBID").agg(countDistinct($"CONS_ID") as "TQDYYHS")

  val aTgTotalArchive = ST_TQ_BYQ.select($"PMS_BYQ_MC" as "PBMC", $"PMS_BYQ_BS" as "PBID", $"ZXMC", $"SSGT", $"GTMC", $"PMS_DWBM" as "DWBM",
    $"PMS_DWMC" as "DWMC", $"BZID", $"TQDZ", $"YX_BYQ_BS" as "TQBS", $"SCCJ", $"CCBH", lit(null) cast "string" as "CCRQ", $"YXZT", $"DQTZ",
    lit(null) cast "string" as "TYRQ", expr("CASE WHEN YX_ZGB_BS = '02' THEN YX_TQ_BS ELSE NULL END") as "ZBID")
    .join(ST_PMS_YX_DW.select($"PMS_DWID" as "DWBM", $"PMS_DWCJ" as "DWJB", $"SJDWID" as "SJDWBM", $"SJDWMC"), Seq("DWBM"), "left_outer")
    .join(TQDYYHS, Seq("PBID"), "left_outer").withColumnRenamed("PBID", "TG_ID").drop("DWBM").persist

  val T_SB_ZWYC_XL = hc.sql("SELECT * FROM PWYW_ARCH.T_SB_ZWYC_XL")

  val ZGB_XL = ST_TQ_BYQ.select($"SSXL" as "XLID", expr("CASE WHEN YX_ZGB_BS = '01' THEN 1 ELSE 0 END") as "GB",
    expr("CASE WHEN YX_ZGB_BS = '02' THEN 1 ELSE 0 END") as "ZB").groupBy($"XLID").agg(sum($"GB") as "GBHS", sum($"ZB") as "ZBHS")

  val aLinesegTotalArchive = T_SB_ZWYC_XL.select($"OBJ_ID" as "XLID", $"XLMC", $"YXBH", lit(null) cast "string" as "ZXMC", $"SSZX",
    $"WHBZ" as "BZID", $"QDDZ" as "SSDZ", $"XLZCD",
    $"JKXLCD", $"DLXLCD", $"JKJXFS", $"DLJXFS", $"ZDYXDL", $"JJDL", $"YXFHXE", $"DQTZ")
    .join(ST_PMS_YX_DW.select($"PMS_DWID", $"SJDWID" as "DWBM"), $"BZID" === $"PMS_DWID", "left_outer").drop("PMS_DWID")
    .join(ST_PMS_YX_DW.select($"PMS_DWID" as "DWBM", $"PMS_DWCJ" as "DWJB", $"SJDWID" as "SJDWBM", $"SJDWMC"), Seq("DWBM"), "left_outer")
    .join(ZGB_XL, Seq("XLID"), "left_outer").withColumnRenamed("XLID", "LINE_SEG_NO").drop("DWBM").persist

  def stat1(filterOrg: (DataFrame) => DataFrame) = {
    filterOrg(ST_TQ_BYQ).select($"PMS_DWBM" as "DWBM", expr("CASE WHEN YX_ZGB_BS = '01' THEN 1 ELSE 0 END") as "GB",
      expr("CASE WHEN YX_ZGB_BS = '02' THEN 1 ELSE 0 END") as "ZB").groupBy($"DWBM").agg(sum($"GB") as "GBTQSL", sum($"ZB") as "ZBTQSL")
  }

  val XLDW = ST_TQ_BYQ.select($"SSXL", $"PMS_DWBM").distinct.persist

  def stat2(filterOrg: (DataFrame) => DataFrame) = {
    filterOrg(XLDW).groupBy($"PMS_DWBM").count.toDF("DWBM", "XLSL")
  }

  def statProv(stat: (DataFrame => DataFrame) => DataFrame) = {
    val fun = (df: DataFrame) => {
      df.withColumn("PMS_DWBM", lit("232AF1D001B65527E055000000000001"))
    }
    stat(fun)
  }

  def statCity(stat: (DataFrame => DataFrame) => DataFrame) = {
    val fun = (df: DataFrame) => {
      df.join(city, $"CITY" === $"PMS_DWBM").withColumn("PMS_DWBM", $"CITY").drop("CITY").unionAll(
        df.join(townOfCity, $"TOWN" === $"PMS_DWBM").withColumn("PMS_DWBM", $"CITY").drop("CITY").drop("TOWN")).unionAll(
          df.join(bzOfCity, $"BZ" === $"PMS_DWBM").withColumn("PMS_DWBM", $"CITY").drop("CITY").drop("BZ"))
    }

    stat(fun)
  }

  def statTown(stat: (DataFrame => DataFrame) => DataFrame) = {
    val fun = (df: DataFrame) => {
      df.join(town, $"TOWN" === $"PMS_DWBM").withColumn("PMS_DWBM", $"TOWN").drop("TOWN").unionAll(
        df.join(bzOfTown, $"BZ" === $"PMS_DWBM").withColumn("PMS_DWBM", $"TOWN").drop("TOWN").drop("BZ"))
    }

    stat(fun)
  }

  def statBZ(stat: (DataFrame => DataFrame) => DataFrame) = {
    val fun = (df: DataFrame) => {
      df.join(bz, $"BZ" === $"PMS_DWBM").withColumn("PMS_DWBM", $"BZ").drop("BZ")
    }

    stat(fun)
  }

  val ZGBSL = statProv(stat1).unionAll(statCity(stat1)).unionAll(statTown(stat1)).unionAll(statBZ(stat1))

  val aFixPreArchive = ST_PMS_YX_DW.select($"PMS_DWID" as "DWBM", $"PMS_DWMC" as "DWMC", $"PMS_DWCJ" as "DWJB", $"SJDWID" as "SJDWBM", $"SJDWMC")
    .join(ZGBSL, Seq("DWBM"), "left_outer").withColumnRenamed("DWBM", "PI_ORG_NO").persist

  val XLSL = statProv(stat2).unionAll(statCity(stat2)).unionAll(statTown(stat2)).unionAll(statBZ(stat2))

  val aFixLinesegArchive = ST_PMS_YX_DW.select($"PMS_DWID" as "DWBM", $"PMS_DWMC" as "DWMC", $"PMS_DWCJ" as "DWJB", $"SJDWID" as "SJDWBM", $"SJDWMC")
    .join(XLSL, Seq("DWBM"), "left_outer").withColumnRenamed("DWBM", "PI_ORG_NO").persist

}