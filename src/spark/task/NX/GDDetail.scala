package spark.task.NX

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import java.util.Date
import spark.util.utils
import java.util.Calendar
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat

case class QXDDETAIL(OBJ_ID: String, TZQXSJ: java.lang.Long, JLDDSJ: java.lang.Long, BXSJ: java.lang.Long, JDDJSJ: java.lang.Long, GDSJ: java.lang.Long,
                      JLXFSJ: java.lang.Long, QXGC: String, DJSJ: java.lang.Long, GZNR: String, JSSJ: java.sql.Timestamp)

class GDDetail(hc: HiveContext, val date: Date) extends Serializable {
  import hc.implicits._

  def execute(statType: String) = {

    val calendar = Calendar.getInstance
    val date1 = utils.sdfDay.format(date)

    val date2 = statType match {
      case "DAY" => {
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, 1)
        utils.sdfDay.format(calendar.getTime)
      }
      case _ => {
        
      }
    }

    val ST_PMS_YX_DW = hc.sql("SELECT * FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

    val ST_PMS_YX_DW1 = ST_PMS_YX_DW.select($"PMS_DWID" as "DWBM", $"PMS_DWMC" as "DWMC", $"PMS_DWCJ" as "DWJB", $"SJDWID" as "SJDWBM", $"SJDWMC")

    val orgRela = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"SJDWID" as "CITY", $"PMS_DWID" as "TOWN")
      .join(ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"SJDWID" as "TOWN", $"PMS_DWID" as "BZ"), Seq("TOWN")).persist

    val city = ST_PMS_YX_DW.filter("PMS_DWCJ = '4' ").select($"PMS_DWID" as "CITY").distinct

    val town = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"PMS_DWID" as "TOWN").distinct

    val bz = ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"PMS_DWID" as "BZ").distinct

    val townOfCity = orgRela.select("CITY", "TOWN").distinct

    val bzOfCity = orgRela.select("CITY", "BZ").distinct

    val bzOfTown = orgRela.select("TOWN", "BZ").distinct

    val T_YJ_GZQX_QXD = hc.read.jdbc(utils.url113, s"""(select OBJ_ID, SSGDDW, LY, SFDWGZ, SFDDXZ, SFQRGZD, SFBHZYYH, SFDX, BXR, 
      BXSJ, BXNR, LXR, LXDZ, LXDH, YHBH, YHMC, YHLX, YHSJ, SSQX, PPHHLX, DJDW, DJBM, DJR, SJDYMC, DYDJ, SYZX, ZXLX, GZSBBH, GZSBMC, 
      GZSBLX_BM, TZSBBH, TZSBMC, CLXCFL, GZDD, GZMS, TZQK, TZSJ, TZYY, GZDZCZW, GZWHCD, GZLX, GZJB, YJFL, EJFL, SJFL, SIJFL, GZXX, 
      JJCD, GZSBCQ, GZYY, GZYYMS, XFCSYY, TQ, CLJG, CLJGMS, JDDJSJ, TZKCSJ, JLDDSJ, HBSJ, TZQXSJ, DWGLSJ, GZXKSJ, GZHBSJ, JLXFSJ, 
      HFSDSJ, ZZXFSJ, ZFDDSJ, DDHTSJ, YJXFSJ, CNDDSJ, BFFHZYSJ,QBFHZYSJ, TDZSC, GDYJ, GDRY, GDSJ, SFGD, ZFYJ, ZFRY, ZFSJ, SFZF, 
      HTYJ, HTSJ, HTRY, SBSSBM, SBYXRY, YXDW, CLDW, QXZDH, HXYWDH, HXYWDLX, QXDZT, GZDJD, GZDWD, SFCS, CSSM, GZLB, SYZXMC, TZSBLX_BM, 
      SFTZ, DJRMC, CLDWMC, FXR, QXDBH, SCGXSJ, SFZXGZ, SQDBH, JXGD_ID, SFDYJDGZ, WJFL, XZDW, AQCSSJ, BBHSBID, BBHSBMC, BBHSBLX, BHDZQK, 
      BHDZSJ, QCFH, SSFH, ZJHM, KHYJ, ZXCLR, ZXCLRMC, JPDRY, JPDRYMC, LXRWZT, XCJDSJ, TDXXBH, TDTZDB_ID, SSBMBZ, SQDZT, BZ, HTYY, FIRSTCFALG, 
      CONTACTNAME, CONTACTWAY, CONTACTTIME, NCONTREASON, DEALRESULT, ISPOWERDUTY, SFZDJD, GZDJD_INTERNET, GZDWD_INTERNET, SLYJ, YSGDDW, 
      QXZHBZ, GZFSSJ, RKSJ, SSTDCID from T_YJ_GZQX_QXD where LY IN ('04', '88') AND JDDJSJ >= to_date('$date1','yyyymmdd') 
    and JDDJSJ < to_date('$date2','yyyymmdd'))  """, utils.cp).repartition(8)

    val T_YJ_GZQX_QXGC = hc.read.jdbc(utils.url113, s"""(select * from scyw.T_YJ_GZQX_QXGC where GCSJ >= to_date('$date1','yyyymmdd') 
    and GCSJ < to_date('$date2','yyyymmdd'))  """, utils.cp).repartition(8)

    val T_YJ_GGSJ_SERVICE_LOG = hc.read.jdbc(utils.url113, s"""(select * from scyw.T_YJ_GGSJ_SERVICE_LOG where sfcg = 1 and jkbm = 'CMS003' and 
    KSSJ >= to_date('$date1','yyyymmdd') and KSSJ < to_date('$date2','yyyymmdd'))  """, utils.cp).repartition(8)

    val T_YJ_DWYJ_BZRWD = hc.read.jdbc(utils.url113, s"""(select * from scyw.T_YJ_DWYJ_BZRWD where RWSLSJ >= to_date('$date1','yyyymmdd') 
    and RWSLSJ < to_date('$date2','yyyymmdd'))  """, utils.cp).repartition(8)

    val T_YJ_DWYJ_XS_ZNXSJH = hc.read.jdbc(utils.url113, s"""(select * from scyw.T_YJ_DWYJ_XS_ZNXSJH where JHXSSJ >= to_date('$date1','yyyymmdd') 
    and JHXSSJ < to_date('$date2','yyyymmdd'))  """, utils.cp).repartition(8)

    val T_YJ_DWYJ_XS_ZWXSJH = hc.read.jdbc(utils.url113, s"""(select * from scyw.T_YJ_DWYJ_XS_ZWXSJH where JHXSSJ >= to_date('$date1','yyyymmdd') 
    and JHXSSJ < to_date('$date2','yyyymmdd'))  """, utils.cp).repartition(8)

    def tsToLong(ts: java.sql.Timestamp): java.lang.Long = if (ts == null) null else ts.getTime
    val tsToLongUDF = udf(tsToLong _)

    val QXGD = T_YJ_GZQX_QXD.select($"OBJ_ID", $"SSGDDW" as "DWBM", expr("CASE WHEN LY = '04' THEN '1' ELSE '2' END") as "GDLY",
      expr("CASE WHEN GZWHCD = '01' THEN '1' WHEN GZWHCD = '02' THEN '2' WHEN GZWHCD = '03' THEN '3' ELSE GZWHCD END") as "YXFW", $"QXDZT",
      tsToLongUDF($"JLXFSJ") as "JLXFSJ", tsToLongUDF($"JDDJSJ") as "JDDJSJ", tsToLongUDF($"JLDDSJ") as "JLDDSJ", tsToLongUDF($"BXSJ") as "BXSJ",
      tsToLongUDF($"GDSJ") as "GDSJ", tsToLongUDF($"TZQXSJ") as "TZQXSJ")
      .persist

    val JXGD = T_YJ_DWYJ_BZRWD.select($"SLBZ_ID" as "DWBM", $"RWZT").join(ST_PMS_YX_DW1.select("DWBM", "SJDWBM"), Seq("DWBM")).withColumn("DWBM", $"SJDWBM")
      .drop("SJDWBM").persist

    val XJGD = T_YJ_DWYJ_XS_ZNXSJH.select($"XSBZ" as "DWBM", $"JHZT").unionAll(T_YJ_DWYJ_XS_ZWXSJH.select($"XSBZ" as "DWBM", $"JHZT"))
      .join(ST_PMS_YX_DW1.select("DWBM", "SJDWBM"), Seq("DWBM")).withColumn("DWBM", $"SJDWBM")
      .drop("SJDWBM").persist

    def statProv = {
      val fun = (df: DataFrame) => {
        df.withColumn("DWBM", lit("232AF1D001B65527E055000000000001"))
      }

      stat(fun(QXGD), fun(JXGD), fun(XJGD)).withColumn("DWJB", lit("3"))
    }

    def statCity = {
      val fun = (df: DataFrame) => {
        df.join(city, $"CITY" === $"DWBM").withColumn("DWBM", $"CITY").drop("CITY").unionAll(
          df.join(townOfCity, $"TOWN" === $"DWBM").withColumn("DWBM", $"CITY").drop("CITY").drop("TOWN")).unionAll(
            df.join(bzOfCity, $"BZ" === $"DWBM").withColumn("DWBM", $"CITY").drop("CITY").drop("BZ"))
      }

      stat(fun(QXGD), fun(JXGD), fun(XJGD)).withColumn("DWJB", lit("4"))
    }

    def statTown = {
      val fun = (df: DataFrame) => {
        df.join(town, $"TOWN" === $"DWBM").withColumn("DWBM", $"TOWN").drop("TOWN").unionAll(
          df.join(bzOfTown, $"BZ" === $"DWBM").withColumn("DWBM", $"TOWN").drop("TOWN").drop("BZ"))
      }

      stat(fun(QXGD), fun(JXGD), fun(XJGD)).withColumn("DWJB", lit("5"))
    }

    def statBZ = {
      val fun = (df: DataFrame) => {
        df.join(bz, $"BZ" === $"DWBM").withColumn("DWBM", $"BZ").drop("BZ")
      }

      stat(fun(QXGD), fun(JXGD), fun(XJGD)).withColumn("DWJB", lit("8"))
    }

    def minus(a: java.sql.Timestamp, b: java.lang.Long): java.lang.Float = {
      if (a == null || b == null) null
      else (a.getTime - b) / 60000.0f
    }

    def minus1(a: java.lang.Long, b: java.lang.Long): java.lang.Float = {
      if (a == null || b == null) null
      else (a - b) / 60000.0f
    }

    def statDetail = {
      val QXGDDETAIL = QXGD.select("OBJ_ID", "TZQXSJ", "JLDDSJ", "BXSJ", "JDDJSJ", "GDSJ", "JLXFSJ")
        .join(T_YJ_GZQX_QXGC.select($"QXD_ID" as "OBJ_ID", $"QXGC", tsToLongUDF($"DJSJ") as "DJSJ", $"GZNR"), Seq("OBJ_ID"), "left_outer")
        .join(T_YJ_GGSJ_SERVICE_LOG.select($"YWZJ" as "OBJ_ID", $"JSSJ"), Seq("OBJ_ID"), "left_outer").as[QXDDETAIL]
        .groupBy($"OBJ_ID")
        .mapGroups(
          (id, i) => {
            val seq = i.toSeq.sortWith((a, b) => if(a.JSSJ == null || b.JSSJ == null) true else a.JSSJ.compareTo(b.JSSJ) > 0)
            val SFZP = if (seq.filter(r => r.QXGC == "27").isEmpty) 0 else 1
            val PDSC = if (seq.filter(r => r.QXGC == "03").isEmpty) null else minus1(seq(0).DJSJ, seq(0).JDDJSJ)
            val GZNR = if (seq.filter(r => r.QXGC == "07").isEmpty) "" else seq(0).GZNR
            val JDCGSJ = seq(0).JSSJ
            val DDXCSC = minus1(seq(0).JLDDSJ, seq(0).BXSJ)
            val JDSC = minus(seq(0).JSSJ, seq(0).JDDJSJ)
            val GDCLSC = minus1(seq(0).GDSJ, seq(0).JDDJSJ)
            val QXSC = minus1(seq(0).JLXFSJ, seq(0).TZQXSJ)
            val QXSC_QXC = minus1(seq(0).JLXFSJ, seq(0).JLDDSJ)
            val QXSC_YHC = minus1(seq(0).JLXFSJ, seq(0).BXSJ)
            (id.getString(0), SFZP, JDCGSJ, DDXCSC, JDSC, GDCLSC, QXSC, QXSC_QXC, QXSC_YHC, PDSC, GZNR)
          })
        .toDF.toDF("OBJ_ID", "SFZP", "JDCGSJ", "DDXCSC", "JDSC", "GDCLSC", "QXSC", "QXSC_QXC", "QXSC_YHC", "PDSC", "GZNR")

      T_YJ_GZQX_QXD.join(QXGDDETAIL, Seq("OBJ_ID"))
    }

    def stat(QXGD: DataFrame, JXGD: DataFrame, XJGD: DataFrame) = {

      val QXGDLOG = QXGD.select("OBJ_ID", "JDDJSJ")
        .join(T_YJ_GGSJ_SERVICE_LOG.select($"YWZJ" as "OBJ_ID", $"JSSJ"), Seq("OBJ_ID"), "right_outer").as[QXGDLOG]
        .groupBy($"OBJ_ID")
        .mapGroups(
          (id, i) => {
            val seq = i.toSeq.sortWith((a, b) =>
              if (a.JSSJ.compareTo(b.JSSJ) > 0) true
              else false)
            val QXSJ = minus(seq(0).JSSJ, seq(0).JDDJSJ)
            (id.getString(0), QXSJ)
          })
        .toDF.toDF("OBJ_ID", "JDCSGDS")

      val QXGDTJ = QXGD.join(QXGDLOG, Seq("OBJ_ID"), "left_outer").groupBy("DWBM", "GDLY", "YXFW").agg(
        count(expr("1")) as "GDZS", sum(expr("CASE WHEN QXDZT = '01' THEN 1 ELSE 0 END")) as "DBZS",
        sum(expr("CASE WHEN QXDZT = '12' THEN 1 ELSE 0 END")) as "YBZS",
        sum(expr("CASE WHEN (QXDZT != '01' and QXDZT != '12') THEN 1 ELSE 0 END")) as "ZZCLZS",
        avg(expr("(JLXFSJ - TZQXSJ) / 3600000")) as "PJQXSC",
        sum(expr("CASE WHEN ((JLDDSJ - BXSJ) / 60000) > 3 THEN 1 ELSE 0 END")) as "DDCSGDS",
        sum(expr("CASE WHEN JDCSGDS > 3 THEN 1 ELSE 0 END")) as "JDCSGDS")
        .join(ST_PMS_YX_DW1, Seq("DWBM"), "left_outer")
        .withColumn("GDLX", lit("3"))

      val JXGDTJ = JXGD.groupBy("DWBM").agg(
        count(expr("1")) as "GDZS", sum(expr("CASE WHEN RWZT = '01' THEN 1 ELSE 0 END")) as "DBZS",
        sum(expr("CASE WHEN RWZT = '03' THEN 1 ELSE 0 END")) as "YBZS",
        sum(expr("CASE WHEN RWZT = '02' THEN 1 ELSE 0 END")) as "ZZCLZS")
        .join(ST_PMS_YX_DW1, Seq("DWBM"), "left_outer")
        .select($"DWBM", lit(null) as "GDLY", lit(null) as "YXFW", $"GDZS", $"DBZS", $"YBZS", $"ZZCLZS", lit(null) as "PJQXSC", lit(null) as "DDCSGDS",
          lit(null) as "JDCSGDS", $"DWMC", $"DWJB", $"SJDWBM", $"SJDWMC", lit("2") as "GDLX")

      val XJGDTJ = XJGD.groupBy("DWBM").agg(
        count(expr("1")) as "GDZS", sum(expr("CASE WHEN JHZT = '02' THEN 1 ELSE 0 END")) as "DBZS",
        sum(expr("CASE WHEN JHZT = '04' THEN 1 ELSE 0 END")) as "YBZS",
        sum(expr("CASE WHEN (JHZT = '01' OR JHZT = '03') THEN 1 ELSE 0 END")) as "ZZCLZS")
        .join(ST_PMS_YX_DW1, Seq("DWBM"), "left_outer")
        .select($"DWBM", lit(null) as "GDLY", lit(null) as "YXFW", $"GDZS", $"DBZS", $"YBZS", $"ZZCLZS", lit(null) as "PJQXSC", lit(null) as "DDCSGDS",
          lit(null) as "JDCSGDS", $"DWMC", $"DWJB", $"SJDWBM", $"SJDWMC", lit("1") as "GDLX")

      QXGDTJ.unionAll(JXGDTJ).unionAll(XJGDTJ)

    }

    val tableName = statType match {
      case "DAY"    => "PWYW_GDTJ_SSXBZ_R"
      case "WEEK"   => "PWYW_GDTJ_SSXBZ_Z"
      case "MONTH"  => "PWYW_GDTJ_SSXBZ_Y"
      case "SEASON" => "PWYW_GDTJ_SSXBZ_J"
      case "HY"     => "PWYW_GDTJ_SSXBZ_BN"
      case "YEAR"   => "PWYW_GDTJ_SSXBZ_N"
    }

    val year = date1.take(4)

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

    val TJJSRQ = {
      calendar.setTime(date)
      calendar.add(Calendar.DAY_OF_YEAR, -1)
      utils.sdfDay.format(calendar.getTime)
    }

//    println(s"$date1     $date2     $tableName     $TJRQ    $TJJSRQ")

    val data = statProv.unionAll(statCity).unionAll(statTown).unionAll(statBZ)

//    statType match {
//      case "DAY"  => NXToOracle.gdtj(data, tableName, TJRQ, true)
//      case "WEEK" => NXToOracle.gdtjW(data, tableName, TJRQ, TJJSRQ)
//      case _      => NXToOracle.gdtj(data, tableName, TJRQ, false)
//    }

    val T_YJ_GZQX_QXPGXX1 = hc.read.jdbc(utils.url113, s"""(select QXD_ID,PGRY_ID from pwyw.T_YJ_GZQX_QXPGXX where SFZF = 'F')""", utils.cp)
      .join(hc.read.jdbc(utils.url113, s"""(select OBJ_ID,FZR,LXDH QXDW_FZR_LXFS,ZYMC,SYGDDW from SCYW.T_YJ_GZQX_QXDWBZ)""", utils.cp).withColumnRenamed("OBJ_ID", "PGRY_ID"),
        Seq("PGRY_ID")).repartition(8)
        
    val T_YJ_GZQX_QXDY = hc.read.jdbc(utils.url113, s"""(select QXDW_ID PGRY_ID,NAMECODE from SCYW.T_YJ_GZQX_QXDY)""", utils.cp)
    val ISC_USER_LOCEXT = hc.read.jdbc(utils.url113, s"""(select SSBMID BZSSGDDW,LOGINNAME NAMECODE from SCYW.ISC_USER_LOCEXT)""", utils.cp)
     
    val T_YJ_GZQX_QXPGXX = T_YJ_GZQX_QXPGXX1.join(T_YJ_GZQX_QXDY,Seq("PGRY_ID")).join(ISC_USER_LOCEXT, Seq("NAMECODE")).drop("NAMECODE").distinct().persist()

    val pbsb = hc.sql("select OBJ_ID SB_ID,SYXZ from pwyw_arch.t_sb_znyc_pdbyq")
      .unionAll(hc.sql("select OBJ_ID SB_ID,SYXZ from pwyw_arch.t_sb_zwyc_zsbyq")).repartition(8)

    val gzyy = hc.read.jdbc(utils.url113, """(SELECT DM GZYY,
  CASE WHEN SJDM_ID = '-1' THEN DM
    ELSE (SELECT DM FROM SCYW.T_YJ_GGSJ_DM WHERE DMFL = '500001' AND SFYX = '1' AND DM_ID = T.SJDM_ID) END GZYYFL
FROM SCYW.T_YJ_GGSJ_DM T WHERE T.DMFL = '500001' AND T.SFYX = '1')""", utils.cp).repartition(8)

    val T_YJ_GZQX_YXYHLB = hc.read.jdbc(utils.url113, s"""(SELECT QXD_ID, YHHH FROM T_YJ_GZQX_YXYHLB)  """, utils.cp).repartition(8)

    val T_YJ_YWZH_TDXXBSB = hc.read.jdbc(utils.url113, s"""(SELECT TDTZDB_ID, OBJ_ID BSB_ID FROM T_YJ_YWZH_TDXXBSB)  """, utils.cp)
      .join(hc.read.jdbc(utils.url113, s"""(SELECT BSB_ID, SB_ID FROM T_YJ_YWZH_TDXXBSSBB)  """, utils.cp), Seq("BSB_ID"))
      .join(pbsb, Seq("SB_ID")).repartition(8)

    val detail1 = statDetail

    val QXDYXYHS = detail1.select($"OBJ_ID" as "QXD_ID").join(T_YJ_GZQX_YXYHLB, Seq("QXD_ID"), "left_outer")
      .groupBy("QXD_ID").agg(sum(expr("case when YHHH is not null then 1 else 0 end")) as "YXYHS").withColumnRenamed("QXD_ID", "OBJ_ID").repartition(8)

    val QXDYXZGBS = detail1.filter($"SSTDCID" isNotNull).select($"SSTDCID" as "TDTZDB_ID").join(T_YJ_YWZH_TDXXBSB, Seq("TDTZDB_ID"), "left_outer")
      .groupBy("TDTZDB_ID").agg(sum(expr("case when SB_ID is not null and SYXZ = '01' then 1 else 0 end")) as "YXZBSL",
        sum(expr("case when SB_ID is not null and SYXZ = '03' then 1 else 0 end")) as "YXGBSL")
      .withColumnRenamed("TDTZDB_ID", "SSTDCID").repartition(8)

    val hour = new SimpleDateFormat("HH").format(new Date)
      
    val detail = detail1.withColumn("JLXFSJ_LONG", tsToLongUDF($"JLXFSJ")).join(QXDYXYHS, Seq("OBJ_ID"), "left_outer").join(QXDYXZGBS, Seq("SSTDCID"), "left_outer")
      .withColumn("DQZT", expr("case when QXDZT = '01' then '01' when QXDZT = '12' then '02' else '03' end")).as("a")
      .join(T_YJ_GZQX_QXPGXX.as("b"), $"a.OBJ_ID" === $"b.QXD_ID", "left_outer")
      .join(T_YJ_GZQX_QXGC.as("c").filter($"QXGC" === "05").select($"QXD_ID"), $"b.QXD_ID" === $"c.QXD_ID", "left_outer")
      .withColumn("DQHJ", expr("""case when a.JDDJSJ IS NOT NULL and b.QXD_ID IS NULL THEN '100'
                                       when a.JLDDSJ IS NULL and b.QXD_ID IS NOT NULL THEN '101'
                                       when a.JLDDSJ IS NOT NULL and c.QXD_ID IS NULL THEN '102'
                                       when a.JLDDSJ IS NOT NULL and c.QXD_ID IS NOT NULL and a.JLXFSJ IS NULL THEN '103'
                                       when a.JLDDSJ IS NOT NULL and a.JLXFSJ IS NOT NULL THEN '104' end """))
      .withColumnRenamed("PGRY_ID", "QXDW_ID")
      .withColumnRenamed("ZYMC", "QXDW_MC")
      //      .withColumn("QXDW_FZR_ID", expr("null"))
      .withColumnRenamed("FZR", "QXDW_FZR_MC")
      //      .withColumnRenamed("LXDH", "QXDW_FZR_LXFS")
      .withColumn("SFJDCS", expr("case when a.JDSC > 3 THEN 'T' else 'F' end"))
      .withColumn("SFDDCS", expr("case when a.CLXCFL = 1 and a.DDXCSC > 45 THEN 'T' when a.CLXCFL = 2 and a.DDXCSC > 90 THEN 'T' when a.CLXCFL = 3 and a.DDXCSC > 120 THEN 'T' else 'F' end"))
      .withColumn("JDFS", expr("case when a.SFZDJD = 'T' THEN '301' when JDDJSJ IS NOT NULL then '302' end"))
      .withColumn("SFWFD", expr(s"case when (a.JLXFSJ_LONG/1000/3600) % 24 >= 10 then 'T' when ${hour} >= '18' and a.JLXFSJ is null then 'T' else 'F' end"))
      .join(gzyy, Seq("GZYY"), "left_outer").drop("QXD_ID").drop("JLXFSJ_LONG")

    NXToOracle.qxdDetail(detail, date1, statType)
  }
}