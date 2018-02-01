package spark.task.NX

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import spark.util.utils
import org.apache.spark.sql.DataFrame

class TYDB(hc: HiveContext, private val date: Date) extends Serializable {
  import hc.implicits._

  private val dateStr = utils.sdfDay.format(date)

  private val ST_PMS_YX_DW = hc.sql("SELECT * FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

  private val orgRela = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"SJDWID" as "CITY", $"PMS_DWID" as "TOWN")
    .join(ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"SJDWID" as "TOWN", $"PMS_DWID" as "BZ"), Seq("TOWN")).persist

  private val city = ST_PMS_YX_DW.filter("PMS_DWCJ = '4' ").select($"PMS_DWID" as "CITY").distinct

  private val town = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"PMS_DWID" as "TOWN").distinct

  private val bz = ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"PMS_DWID" as "BZ").distinct

  private val townOfCity = orgRela.select("CITY", "TOWN").distinct

  private val bzOfCity = orgRela.select("CITY", "BZ").distinct

  private val bzOfTown = orgRela.select("TOWN", "BZ").distinct

  private val GBZRL_0 = hc.sql("SELECT PMS_EDRL, PMS_DWBM, CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.ST_TQ_BYQ WHERE YX_ZGB_BS = '01' ").persist
  private val GBZRL = GBZRL_0.unionAll(GBZRL_0.select("PMS_EDRL","PMS_DWBM").withColumn("CNW", lit("1"))).persist

  private val DYYHZS_0 = hc.sql("""SELECT CMP.CONS_ID, PMS_DWBM, CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.C_MP_DET CMP , PWYW_ARCH.ST_TQ_BYQ GTG
    WHERE CMP.TG_ID = GTG.YX_TQ_BS AND CMP.STATUS_CODE IN ('01','02') AND CMP.USAGE_TYPE_CODE IN ('02')""").persist
  private val DYYHZS = DYYHZS_0.unionAll(DYYHZS_0.select("CONS_ID","PMS_DWBM").withColumn("CNW", lit("1"))).persist

  private val PDXLPJGDBJ_0 = hc.sql("""SELECT KXGDBJ/1000 KXGDBJ, KXID, SJDWID AS PMS_DWBM, CASE WHEN XL.SFNW = '0' THEN '2' ELSE '3' END CNW FROM
    PWYW_ARCH.T_PWYX_KXYXBJ BJ , PWYW_ARCH.T_SB_ZWYC_XL XL, PWYW_ARCH.ST_PMS_YX_DW DW WHERE BJ.KXID = XL.OBJ_ID AND XL.WHBZ = DW.PMS_DWID and KXGDBJ > 0""")
  private val PDXLPJGDBJ = PDXLPJGDBJ_0.unionAll(PDXLPJGDBJ_0.select("KXGDBJ","KXID","PMS_DWBM").withColumn("CNW", lit("1"))).persist

  private val DYYHJCGYPBSL_0 = hc.sql("""SELECT SSPB, PMS_DWBM, CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_PWYX_DYYHXX_JLB JB ,PWYW_ARCH.ST_TQ_BYQ TQ
    WHERE SZFS IN ('01','02') AND JB.SSPB = TQ.PMS_BYQ_BS""").persist
  private val DYYHJCGYPBSL = DYYHJCGYPBSL_0.unionAll(DYYHJCGYPBSL_0.select("SSPB","PMS_DWBM").withColumn("CNW", lit("1"))).persist

  private val GBZSL_0 = hc.sql("""SELECT SJDWID as PMS_DWBM, CASE WHEN PD.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZNYC_PDBYQ PD, PWYW_ARCH.ST_PMS_YX_DW DW WHERE FBZT = '发布'
    AND ZCXZ <> '05' AND ZCXZ IS NOT NULL AND YXZT = '20' and SYXZ = '03' AND DYDJ IN ('21', '22', '24') AND DW.PMS_DWID = PD.WHBZ""")
    .unionAll(hc.sql("""SELECT SJDWID AS PMS_DWBM, CASE WHEN PD.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSBYQ PD, PWYW_ARCH.ST_PMS_YX_DW DW WHERE FBZT = '发布'
      AND ZCXZ <> '05' AND ZCXZ IS NOT NULL AND YXZT = '20' and SYXZ = '03' AND DYDJ IN ('21', '22', '24') AND DW.PMS_DWID = PD.WHBZ""")).persist
  private val GBZSL = GBZSL_0.unionAll(GBZSL_0.select("PMS_DWBM").withColumn("CNW", lit("1"))).persist


  private val YDYGBZS_0 = hc.sql("SELECT PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.ST_TQ_BYQ WHERE YX_ZGB_BS = '01' ").persist
  private val YDYGBZS = YDYGBZS_0.unionAll(YDYGBZS_0.select("PMS_DWBM").withColumn("CNW", lit("1"))).persist

  private val GBZS_0 = hc.sql("SELECT PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.ST_TQ_BYQ WHERE YX_ZGB_BS = '01' ").persist
  private val GBZS = GBZS_0.unionAll(GBZS_0.select("PMS_DWBM").withColumn("CNW", lit("1"))).persist

  private val WCYXSJCJGBZS_0 = hc.sql(s"""SELECT DISTINCT A.PMS_BYQ_BS, PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.E_MP_CUR_CURVE E, PWYW_ARCH.ST_TQ_BYQ A WHERE E.TG_ID = A.YX_TQ_BS
    AND YX_ZGB_BS = '01' AND E.dt = '$dateStr' """)
    .unionAll(hc.sql(s"""SELECT DISTINCT A.PMS_BYQ_BS, PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.E_MP_VOL_CURVE E, PWYW_ARCH.ST_TQ_BYQ A WHERE E.TG_ID = A.YX_TQ_BS
      AND YX_ZGB_BS = '01' AND E.dt = '$dateStr' """))
    .unionAll(hc.sql(s"""SELECT DISTINCT A.PMS_BYQ_BS, PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.E_MP_POWER_CURVE E, PWYW_ARCH.ST_TQ_BYQ A WHERE E.TG_ID = A.YX_TQ_BS AND
      YX_ZGB_BS = '01' AND E.dt = '$dateStr' """)).persist
  private val WCYXSJCJGBZS = WCYXSJCJGBZS_0.unionAll(WCYXSJCJGBZS_0.select("PMS_BYQ_BS","PMS_DWBM").withColumn("CNW", lit("1"))).persist


  private val SJCJSJLDS_0 = hc.sql(s"""SELECT PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW,I1,I2,I3,I4,I5,I6,I7,I8,I9,I10,I11,I12,I13,I14,I15,I16,I17,I18,I19,I20,I21,I22,I23,I24,I25,I26,I27,I28,I29,I30,I31,I32,
    I33,I34,I35,I36,I37,I38,I39,I40,I41,I42,I43,I44,I45,I46,I47,I48,I49,I50,I51,I52,I53,I54,I55,I56,I57,I58,I59,I60,I61,I62,I63,I64,I65,I66,I67,I68,I69,I70,
    I71,I72,I73,I74,I75,I76,I77,I78,I79,I80,I81,I82,I83,I84,I85,I86,I87,I88,I89,I90,I91,I92,I93,I94,I95,I96
    FROM PWYW_ARCH.E_MP_CUR_CURVE E ,PWYW_ARCH.ST_TQ_BYQ A WHERE E.TG_ID = A.YX_TQ_BS AND YX_ZGB_BS = '01' AND E.dt = '$dateStr' """).map {
    r =>
      var count = 0
      for (i <- 2 to 97)
        if (!r.isNullAt(i)) count += 1
      (r.getString(0), count,r.getString(1))
  }.toDF("PMS_DWBM", "SJCJSJLDS", "CNW").unionAll(hc.sql(s"""SELECT PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW,U1,U2,U3,U4,U5,U6,U7,U8,U9,U10,U11,U12,U13,U14,U15,U16,U17,U18,U19,U20,U21,U22,U23,U24,U25,
    U26,U27,U28,U29,U30,U31,U32,U33,U34,U35,U36,U37,U38,U39,U40,U41,U42,U43,U44,U45,U46,U47,U48,U49,U50,U51,U52,U53,U54,U55,U56,U57,U58,U59,U60,U61,U62,U63,
    U64,U65,U66,U67,U68,U69,U70,U71,U72,U73,U74,U75,U76,U77,U78,U79,U80,U81,U82,U83,U84,U85,U86,U87,U88,U89,U90,U91,U92,U93,U94,U95,U96
    FROM PWYW_ARCH.E_MP_VOL_CURVE E ,PWYW_ARCH.ST_TQ_BYQ A WHERE E.TG_ID = A.YX_TQ_BS AND YX_ZGB_BS = '01' AND E.dt = '$dateStr' """).map {
    r =>
      var count = 0
      for (i <- 2 to 97)
        if (!r.isNullAt(i)) count += 1
      (r.getString(0), count,r.getString(1))
  }.toDF("PMS_DWBM", "SJCJSJLDS", "CNW")).unionAll(hc.sql(s"""SELECT PMS_DWBM,CASE WHEN CNW = '0' THEN '2' ELSE '3' END CNW,P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,P15,P16,P17,P18,P19,P20,P21,P22,P23,
    P24,P25,P26,P27,P28,P29,P30,P31,P32,P33,P34,P35,P36,P37,P38,P39,P40,P41,P42,P43,P44,P45,P46,P47,P48,P49,P50,P51,P52,P53,P54,P55,P56,P57,P58,P59,P60,P61,
    P62,P63,P64,P65,P66,P67,P68,P69,P70,P71,P72,P73,P74,P75,P76,P77,P78,P79,P80,P81,P82,P83,P84,P85,P86,P87,P88,P89,P90,P91,P92,P93,P94,P95,P96
    FROM PWYW_ARCH.E_MP_POWER_CURVE E ,PWYW_ARCH.ST_TQ_BYQ A WHERE E.TG_ID = A.YX_TQ_BS AND YX_ZGB_BS = '01' AND E.dt = '$dateStr' """).map {
    r =>
      var count = 0
      for (i <- 2 to 97)
        if (!r.isNullAt(i)) count += 1
      (r.getString(0), count,r.getString(1))
  }.toDF("PMS_DWBM", "SJCJSJLDS", "CNW")).persist
  private val SJCJSJLDS = SJCJSJLDS_0.unionAll(SJCJSJLDS_0.select("PMS_DWBM","SJCJSJLDS").withColumn("CNW", lit("1"))).persist

  val GT_XL_GX_0 = hc.sql("select GT.OBJ_ID SSGT,XL.OBJ_ID XLID,CASE WHEN XL.SFNW = '0' THEN '2' ELSE '3' END CNW from pwyw_arch.t_sb_zwyc_gt gt,pwyw_arch.t_sb_zwyc_xl xl where gt.SSXL = xl.OBJ_ID and xl.JSFS in (1,2)").persist()
  val GT_XL_GX = GT_XL_GX_0.unionAll(GT_XL_GX_0.select("SSGT","XLID").withColumn("CNW", lit("1"))).persist

  val JKXLLLKGZS_0 = hc.sql("SELECT D.SSGT,DW.SJDWID AS PMS_DWBM,CASE WHEN D.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSDLQ D,PWYW_ARCH.ST_PMS_YX_DW DW WHERE KGZY = '03' AND DW.PMS_DWID = D.WHBZ")
    .unionAll(hc.sql("SELECT D.SSGT,DW.SJDWID AS PMS_DWBM,CASE WHEN D.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSGLKG D,PWYW_ARCH.ST_PMS_YX_DW DW WHERE KGZY = '03' AND DW.PMS_DWID = D.WHBZ"))
    .unionAll(hc.sql("SELECT D.SSGT,DW.SJDWID AS PMS_DWBM,CASE WHEN D.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSFHKG D, PWYW_ARCH.ST_PMS_YX_DW DW WHERE KGZY = '03' AND DW.PMS_DWID = D.WHBZ")).persist
  val JKXLLLKGZS_1 = JKXLLLKGZS_0.unionAll(JKXLLLKGZS_0.select("SSGT","PMS_DWBM").withColumn("CNW", lit("1"))).persist

  val JKXLLLKGZS = JKXLLLKGZS_1.as("KG").join(GT_XL_GX.as("GT"), Seq("SSGT","CNW")).select("PMS_DWBM","CNW").persist

  val JKXLFDKGZS_0 = hc.sql("SELECT D.SSGT,DW.SJDWID AS PMS_DWBM,CASE WHEN D.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSDLQ D, PWYW_ARCH.ST_PMS_YX_DW DW WHERE KGZY = '01' AND DW.PMS_DWID = D.WHBZ")
    .unionAll(hc.sql("SELECT D.SSGT,DW.SJDWID AS PMS_DWBM,CASE WHEN D.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSGLKG D, PWYW_ARCH.ST_PMS_YX_DW DW WHERE KGZY = '01' AND DW.PMS_DWID = D.WHBZ"))
    .unionAll(hc.sql("SELECT D.SSGT,DW.SJDWID AS PMS_DWBM,CASE WHEN D.SFNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW_ARCH.T_SB_ZWYC_ZSFHKG D, PWYW_ARCH.ST_PMS_YX_DW DW WHERE KGZY = '01' AND DW.PMS_DWID = D.WHBZ")).persist
  val JKXLFDKGZS_1 = JKXLFDKGZS_0.unionAll(JKXLFDKGZS_0.select("SSGT","PMS_DWBM").withColumn("CNW", lit("1"))).persist

  val JKXLFDKGZS = JKXLFDKGZS_1.as("KG").join(GT_XL_GX.as("GT"), Seq("SSGT","CNW")).select("PMS_DWBM","CNW").persist

  val DKXTS = hc.sql("SELECT DW.SJDWID AS PMS_DWBM,'1' CNW FROM PWYW_ARCH.T_SB_ZWYC_DKX D, PWYW_ARCH.ST_PMS_YX_DW DW WHERE DW.PMS_DWID = D.WHBZ").persist

  val FZYCGBZS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBZGZ_MX CC ,PWYW_ARCH.ST_TQ_BYQ A
                          WHERE CC.PBID = A.PMS_BYQ_BS AND dt = '$dateStr' AND FSRQ = '$dateStr' AND CC.PBLX = '1' AND FHL > 150""").persist
  val FZYCGBZS = FZYCGBZS_0.unionAll(FZYCGBZS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  val ZZGBZS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBYDYC CC ,PWYW_ARCH.ST_TQ_BYQ A
                          WHERE CC.PBID = A.PMS_BYQ_BS AND dt = '$dateStr' AND DQGJFSSJ = '$dateStr' AND CC.PBLX = '1' AND GJBM = '00139' """).persist
  val ZZGBZS = ZZGBZS_0.unionAll(ZZGBZS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  val GZGBZS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBYDYC CC ,PWYW_ARCH.ST_TQ_BYQ A
                          WHERE CC.PBID = A.PMS_BYQ_BS AND  dt = '$dateStr' AND DQGJFSSJ = '$dateStr' AND CC.PBLX = '1' AND GJBM in ('00130','00131','00132') """).persist
  val GZGBZS = GZGBZS_0.unionAll(GZGBZS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  val SXBPHYCGBTS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBYDYC CC ,PWYW_ARCH.ST_TQ_BYQ A
                               WHERE CC.PBID = A.PMS_BYQ_BS AND  dt = '$dateStr' AND DQGJFSSJ = '$dateStr' AND CC.PBLX = '1' AND GJBM = '00112' """).persist
  val SXBPHYCGBTS = SXBPHYCGBTS_0.unionAll(SXBPHYCGBTS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  val GDYGBTS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBYDYC CC ,PWYW_ARCH.ST_TQ_BYQ A
                           WHERE CC.PBID = A.PMS_BYQ_BS AND  dt = '$dateStr' AND DQGJFSSJ = '$dateStr' AND CC.PBLX = '1' AND GJBM = '00110' """).persist
  val GDYGBTS = GDYGBTS_0.unionAll(GDYGBTS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  val DDYGBTS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBYDYC CC ,PWYW_ARCH.ST_TQ_BYQ A
                           WHERE CC.PBID = A.PMS_BYQ_BS AND  dt = '$dateStr' AND DQGJFSSJ = '$dateStr' AND CC.PBLX = '1' AND GJBM = '00111' """).persist
  val DDYGBTS = DDYGBTS_0.unionAll(DDYGBTS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  val YCDYGBTS_0 = hc.sql(s"""SELECT PBID, DWBM AS PMS_DWBM, CASE WHEN A.CNW = '0' THEN '2' ELSE '3' END CNW FROM PWYW.PWYW_PBYDYC CC ,PWYW_ARCH.ST_TQ_BYQ A
                            WHERE CC.PBID = A.PMS_BYQ_BS AND  dt = '$dateStr' AND DQGJFSSJ = '$dateStr' AND CC.PBLX = '1'AND GJBM in ('00110','00111') """).persist
  val YCDYGBTS = YCDYGBTS_0.unionAll(YCDYGBTS_0.select("PBID","PMS_DWBM").withColumn("CNW",lit("1"))).persist

  def statProv = {
    val fun = (df: DataFrame) => {
      df.withColumn("PMS_DWBM",lit("232AF1D001B65527E055000000000001"))
    }

    stat(fun(GBZRL), fun(DYYHZS), fun(PDXLPJGDBJ), fun(DYYHJCGYPBSL), fun(GBZSL), fun(YDYGBZS), fun(GBZS), fun(WCYXSJCJGBZS), fun(SJCJSJLDS),
      fun(JKXLLLKGZS), fun(JKXLFDKGZS), fun(DKXTS), fun(FZYCGBZS), fun(ZZGBZS), fun(GZGBZS), fun(SXBPHYCGBTS), fun(GDYGBTS), fun(DDYGBTS), fun(YCDYGBTS))
      .withColumn("DWJB", lit("3"))
  }

  def statCity = {
    val fun = (df: DataFrame) => {
      df.join(city, $"CITY" === $"PMS_DWBM").withColumn("PMS_DWBM", $"CITY").drop("CITY").unionAll(
        df.join(townOfCity, $"TOWN" === $"PMS_DWBM").withColumn("PMS_DWBM", $"CITY").drop("CITY").drop("TOWN")).unionAll(
          df.join(bzOfCity, $"BZ" === $"PMS_DWBM").withColumn("PMS_DWBM", $"CITY").drop("CITY").drop("BZ"))
    }

    stat(fun(GBZRL), fun(DYYHZS), fun(PDXLPJGDBJ), fun(DYYHJCGYPBSL), fun(GBZSL), fun(YDYGBZS), fun(GBZS), fun(WCYXSJCJGBZS), fun(SJCJSJLDS),
      fun(JKXLLLKGZS), fun(JKXLFDKGZS), fun(DKXTS), fun(FZYCGBZS), fun(ZZGBZS), fun(GZGBZS), fun(SXBPHYCGBTS), fun(GDYGBTS), fun(DDYGBTS), fun(YCDYGBTS))
      .withColumn("DWJB", lit("4"))
  }

  def statTown = {
    val fun = (df: DataFrame) => {
      df.join(town, $"TOWN" === $"PMS_DWBM").withColumn("PMS_DWBM", $"TOWN").drop("TOWN").unionAll(
        df.join(bzOfTown, $"BZ" === $"PMS_DWBM").withColumn("PMS_DWBM", $"TOWN").drop("TOWN").drop("BZ"))
    }

    stat(fun(GBZRL), fun(DYYHZS), fun(PDXLPJGDBJ), fun(DYYHJCGYPBSL), fun(GBZSL), fun(YDYGBZS), fun(GBZS), fun(WCYXSJCJGBZS), fun(SJCJSJLDS),
      fun(JKXLLLKGZS), fun(JKXLFDKGZS), fun(DKXTS), fun(FZYCGBZS), fun(ZZGBZS), fun(GZGBZS), fun(SXBPHYCGBTS), fun(GDYGBTS), fun(DDYGBTS), fun(YCDYGBTS))
      .withColumn("DWJB", lit("5"))
  }

  private def stat(GBZRL1: DataFrame, DYYHZS1: DataFrame, PDXLPJGDBJ1: DataFrame, DYYHJCGYPBSL1: DataFrame, GBZSL1: DataFrame, YDYGBZS1: DataFrame, GBZS1: DataFrame,
                   WCYXSJCJGBZS1: DataFrame, SJCJSJLDS1: DataFrame, JKXLLLKGZS1: DataFrame, JKXLFDKGZS1: DataFrame, DKXTS1: DataFrame, FZYCGBZS1: DataFrame,
                   ZZGBZS1: DataFrame, GZGBZS1: DataFrame, SXBPHYCGBTS1: DataFrame, GDYGBTS1: DataFrame, DDYGBTS1: DataFrame, YCDYGBTS1: DataFrame) = {
    val GBZRL_0 = GBZRL1.groupBy("PMS_DWBM","CNW").agg(sum($"PMS_EDRL") as "GBZRL")
    val GBZRL = GBZRL_0.as("GB").join(ST_PMS_YX_DW.as("DW"),$"GB.PMS_DWBM" === $"DW.PMS_DWID")
                      .select($"GB.PMS_DWBM",$"GB.CNW",$"GB.GBZRL",$"DW.PMS_DWMC",$"DW.SJDWID",$"DW.SJDWMC")

    val DYYHZS = DYYHZS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"CONS_ID") as "DYYHZS")

    val PDXLPJGDBJ = PDXLPJGDBJ1.groupBy("PMS_DWBM","CNW").agg(sum($"KXGDBJ") as "PDXLGDBJH", countDistinct($"KXID") as "PDXLZTS").persist()

    val DYYHJCGYPBSL = DYYHJCGYPBSL1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"SSPB") as "DYYHJCGYPBSL")
    val GBZSL = GBZSL1.groupBy("PMS_DWBM","CNW").count.toDF("PMS_DWBM","CNW", "GBZSL")
    val YDYGBZS = YDYGBZS1.groupBy("PMS_DWBM","CNW").count.toDF("PMS_DWBM","CNW", "YDYGBZS")
    val GBZS = GBZS1.groupBy("PMS_DWBM","CNW").count.toDF("PMS_DWBM","CNW", "GBZS")
    val WCYXSJCJGBZS = WCYXSJCJGBZS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PMS_BYQ_BS") as "WCYXSJCJGBZS")
    val SJCJSJLDS = SJCJSJLDS1.groupBy("PMS_DWBM","CNW").agg(sum($"SJCJSJLDS") as "SJCJSJLDS")
    val JKXLLLKGZS = JKXLLLKGZS1.groupBy("PMS_DWBM","CNW").count.toDF("PMS_DWBM","CNW", "JKXLLLKGZS")
    val JKXLFDKGZS = JKXLFDKGZS1.groupBy("PMS_DWBM","CNW").count.toDF("PMS_DWBM","CNW", "JKXLFDKGZS")
    val DKXTS = DKXTS1.groupBy("PMS_DWBM","CNW").count.toDF("PMS_DWBM","CNW", "DKXTS")
    val FZYCGBZS = FZYCGBZS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "FZYCGBZS")
    val ZZGBZS = ZZGBZS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "ZZGBZS")
    val GZGBZS = GZGBZS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "GZGBZS")
    val SXBPHYCGBTS = SXBPHYCGBTS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "SXBPHYCGBTS")
    val GDYGBTS = GDYGBTS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "GDYGBTS")
    val DDYGBTS = DDYGBTS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "DDYGBTS")
    val YCDYGBTS = YCDYGBTS1.groupBy("PMS_DWBM","CNW").agg(countDistinct($"PBID") as "YCDYGBTS")

    //    GBZRL.join(DYYHZS, Seq("PMS_DWBM")).join(PDXLPJGDBJ, Seq("PMS_DWBM"), "outer").join(GBZSL, Seq("PMS_DWBM"), "outer")
    //      .join(DYYHJCGYPBSL, Seq("PMS_DWBM"), "outer")
    //      .join(YDYGBZS.join(GBZS, Seq("PMS_DWBM")), Seq("PMS_DWBM"), "outer")
    //      .join(WCYXSJCJGBZS, Seq("PMS_DWBM"), "outer").join(SJCJSJLDS, Seq("PMS_DWBM"), "outer")
    //      .join(JKXLLLKGZS.join(JKXLFDKGZS, Seq("PMS_DWBM")).join(DKXTS, Seq("PMS_DWBM")), Seq("PMS_DWBM"), "outer")
    //      .join(FZYCGBZS.join(ZZGBZS, Seq("PMS_DWBM")).join(GZGBZS, Seq("PMS_DWBM")), Seq("PMS_DWBM"), "outer")
    //      .join(SXBPHYCGBTS, Seq("PMS_DWBM"), "outer")
    //      .join(GDYGBTS.join(DDYGBTS, Seq("PMS_DWBM")).join(YCDYGBTS, Seq("PMS_DWBM")), Seq("PMS_DWBM"), "outer")
    GBZRL.join(DYYHZS, Seq("PMS_DWBM","CNW"), "outer").join(PDXLPJGDBJ, Seq("PMS_DWBM","CNW"), "outer").join(DYYHJCGYPBSL, Seq("PMS_DWBM","CNW"), "outer")
      .join(GBZSL, Seq("PMS_DWBM","CNW"), "outer").join(YDYGBZS, Seq("PMS_DWBM","CNW"), "outer").join(GBZS, Seq("PMS_DWBM","CNW"), "outer")
      .join(WCYXSJCJGBZS, Seq("PMS_DWBM","CNW"), "outer").join(SJCJSJLDS, Seq("PMS_DWBM","CNW"), "outer").join(JKXLLLKGZS, Seq("PMS_DWBM","CNW"), "outer")
      .join(JKXLFDKGZS, Seq("PMS_DWBM","CNW"), "outer").join(DKXTS, Seq("PMS_DWBM","CNW"), "outer").join(FZYCGBZS, Seq("PMS_DWBM","CNW"), "outer")
      .join(ZZGBZS, Seq("PMS_DWBM","CNW"), "outer").join(GZGBZS, Seq("PMS_DWBM","CNW"), "outer").join(SXBPHYCGBTS, Seq("PMS_DWBM","CNW"), "outer")
      .join(GDYGBTS, Seq("PMS_DWBM","CNW"), "outer").join(DDYGBTS, Seq("PMS_DWBM","CNW"), "outer").join(YCDYGBTS, Seq("PMS_DWBM","CNW"), "outer")
      .na.fill(0)
      .select($"PMS_DWBM" as "DWBM", $"GBZRL", $"DYYHZS", expr("GBZRL / DYYHZS") as "DYYHHJPBRL", $"PDXLGDBJH", $"PDXLZTS", expr("PDXLGDBJH/PDXLZTS") as "PDXLPJGDBJ",
        $"DYYHJCGYPBSL", $"GBZSL", expr("DYYHJCGYPBSL / GBZSL * 100") as "DYYHJCDFGL", $"YDYGBZS", $"GBZS", expr("case when YDYGBZS * 2 > (GBZS + GBZSL) then 100 else 200 * YDYGBZS / (GBZS + GBZSL) end") as "GBYPDYYZL",
        $"WCYXSJCJGBZS", expr("WCYXSJCJGBZS / GBZSL * 100") as "GBYXSJCJL", $"SJCJSJLDS", expr("GBZSL * 8 * 96") as "YCJSJLZDS",
        expr("SJCJSJLDS / (GBZSL * 8 * 96) * 100") as "GBYXSJWZL", $"JKXLLLKGZS", $"JKXLFDKGZS", $"DKXTS",
        expr("JKXLLLKGZS * 200 / (JKXLFDKGZS + DKXTS)") as "JKXLWJHGL", $"FZYCGBZS", $"ZZGBZS", $"GZGBZS",
        expr("70 * (1 - FZYCGBZS / GBZSL) + 30 * (1 - (ZZGBZS + GZGBZS)/ GBZSL)") as "GBFZHGL", $"SXBPHYCGBTS",
        expr("(1 - SXBPHYCGBTS / GBZSL) * 100") as "GBSXBPHHLL", $"GDYGBTS", $"DDYGBTS", $"YCDYGBTS",
        expr("30 * (1 - GDYGBTS / GBZSL) + 30 * (1 - DDYGBTS / GBZSL)+ 40 * (1 - YCDYGBTS / GBZSL)") as "GBDYHLL", $"CNW",$"PMS_DWMC",$"SJDWID",$"SJDWMC")
      .withColumn("GBYXSJHLL", expr("0.5 * GBFZHGL + 0.3 * GBSXBPHHLL + 0.2 * GBDYHLL"))
  }

}