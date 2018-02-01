package spark.task.NX

import java.util.Calendar
import java.util.Date

import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

import spark.util.utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window

case class comm_fac_gk(COMM_FAC_ID: java.math.BigDecimal, OCCUR_TIME: java.lang.Long, STATUS: java.lang.Integer)

class PDZB(hc: HiveContext, val date: Date) extends Serializable {
  import hc.implicits._

  private val PWYW_DMS_TERMINAL_INFO = hc.read.jdbc(utils.url113, "PWYW.PWYW_DMS_TERMINAL_INFO", utils.cp).persist

  private val PWYW_DMS_BRK_INFO = hc.read.jdbc(utils.url113, "PWYW.PWYW_DMS_BRK_INFO", utils.cp).persist

  private val ST_PMS_YX_DW = hc.sql("SELECT * FROM PWYW_ARCH.ST_PMS_YX_DW").persist()

  private val DW_ARCH = ST_PMS_YX_DW.filter("PMS_DWCJ in (3,4,5,8)").select($"PMS_DWID" as "DWBM",
    $"PMS_DWMC" as "DWMC", $"PMS_DWCJ" as "DWJB").persist()

  private val T_SB_ZWYC_XL = hc.sql("SELECT * FROM PWYW_ARCH.T_SB_ZWYC_XL").filter("XLXZ = 5")
    .select($"OBJ_ID", $"WHBZ", $"SFNW")
    .join(ST_PMS_YX_DW.select($"SJDWID" as "DWBM", $"PMS_DWID" as "WHBZ"), Seq("WHBZ"))

  private val orgRela = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"SJDWID" as "CITY", $"PMS_DWID" as "TOWN")
    .join(ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"SJDWID" as "TOWN", $"PMS_DWID" as "BZ"), Seq("TOWN")).persist

  private val city = ST_PMS_YX_DW.filter("PMS_DWCJ = '4' ").select($"PMS_DWID" as "CITY").distinct

  private val town = ST_PMS_YX_DW.filter("PMS_DWCJ = '5' ").select($"PMS_DWID" as "TOWN").distinct

  private val bz = ST_PMS_YX_DW.filter("PMS_DWCJ = '8' ").select($"PMS_DWID" as "BZ").distinct

  private val townOfCity = orgRela.select("CITY", "TOWN").distinct

  private val bzOfCity = orgRela.select("CITY", "BZ").distinct

  private val bzOfTown = orgRela.select("TOWN", "BZ").distinct

  def execute(statType: String) = {

    val calendar = Calendar.getInstance
    val date2 = utils.sdfDay.format(date)

    var tableName = ""
    val date1 = statType match {
      case "DAY" => {
        tableName = "_R"
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, -1)
        utils.sdfDay.format(calendar.getTime)
      }
      case "WEEK" => {
        tableName = "_Z"
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, -7)
        utils.sdfDay.format(calendar.getTime)
      }
      case "MONTH" => {
        tableName = "_Y"
        calendar.setTime(date)
        calendar.add(Calendar.MONTH, -1)
        utils.sdfDay.format(calendar.getTime)
      }
      case "SEASON" => {
        tableName = "_J"
        calendar.setTime(date)
        calendar.add(Calendar.MONTH, -3)
        utils.sdfDay.format(calendar.getTime)
      }
      case "HY" => {
        tableName = "_BN"
        calendar.setTime(date)
        calendar.add(Calendar.MONTH, -6)
        utils.sdfDay.format(calendar.getTime)
      }
      case "YEAR" => {
        tableName = "_N"
        calendar.setTime(date)
        calendar.add(Calendar.YEAR, -1)
        utils.sdfDay.format(calendar.getTime)
      }
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

    def tsToLong(ts: java.sql.Timestamp): java.lang.Long = if (ts == null) null else ts.getTime
    val tsToLongUDF = udf(tsToLong _)

    def minus(a: java.lang.Long, b: java.lang.Long): java.lang.Float = {
      if (a == null || b == null) null
      else (a - b) / 3600000.0f
    }

    val zero1 = utils.sdfDay.parse(date2).getTime
    val zero = utils.sdfDay.parse(date1).getTime

    val YZXSC = (zero1 - zero) / 3600 / 1000

    val fuck = hc.read.jdbc(utils.urlSGPMS, s"""(select max(occur_time) as OCCUR_TIME,COMM_FAC_ID,STATUS from 
      comm_fac_gk k where status in (1,4) group by comm_fac_id, status)""", utils.cpSGPMS)
      .select($"COMM_FAC_ID", tsToLongUDF($"OCCUR_TIME") as "OCCUR_TIME", $"STATUS" cast "int").as[comm_fac_gk]
      .groupBy($"COMM_FAC_ID").mapGroups {
        (id, i) =>
          {
            val seq = i.toSeq.sortBy(_.OCCUR_TIME)
            seq.last
          }
      }
      .filter(r => r.OCCUR_TIME < zero).map {
        r =>
          if (r.STATUS == 1)
            (r.COMM_FAC_ID, YZXSC.toFloat, 0f, YZXSC)
          else
            (r.COMM_FAC_ID, 0f, YZXSC.toFloat, YZXSC)
      }.toDF.toDF("COMM_FAC_ID", "ZXSC", "LXLXZDSC", "YZXSC")

    val PWYW_DMS_TMNL_COMM_DET = hc.read.jdbc(utils.urlSGPMS, s"""(select COMM_FAC_ID, OCCUR_TIME, STATUS 
      from comm_fac_gk where occur_time >= to_date('$date1','yyyymmdd') and occur_time < to_date('$date2','yyyymmdd')
       and STATUS in (1,4))""", utils.cpSGPMS)
      .select($"COMM_FAC_ID", tsToLongUDF($"OCCUR_TIME") as "OCCUR_TIME", $"STATUS" cast "int").as[comm_fac_gk]
      .groupBy($"COMM_FAC_ID").mapGroups(
        (id, i) => {
          val seq = i.toSeq.sortBy(_.OCCUR_TIME)
          val seq1 = new scala.collection.mutable.ArrayBuffer[comm_fac_gk]
          var flag = -1
          var tmp: comm_fac_gk = null
          for (i <- seq) {
            if (i.STATUS != flag) {
              flag = i.STATUS
              if (i.STATUS == 1) {
                if (tmp != null) seq1 += tmp
                seq1 += i
              } else {
                tmp = i
              }
            } else {
              if (i.STATUS == 4) tmp = i
            }
          } // distinct
          if (seq(seq.size - 1).STATUS == 4) seq1 += seq(seq.size - 1) //last is 4

          val seq2 = new scala.collection.mutable.ArrayBuffer[comm_fac_gk]
          seq1.copyToBuffer(seq2)

          if (seq1(0).STATUS == 4)
            seq1.+=:(comm_fac_gk(seq1(0).COMM_FAC_ID, zero, 1))
          if (seq1(seq1.size - 1).STATUS == 1)
            seq1 += comm_fac_gk(seq1(0).COMM_FAC_ID, zero1, 4)
          var zxsc = 0f
          for (i <- 0 to seq1.size - 1 if (i % 2 == 0))
            zxsc += minus(seq1(i + 1).OCCUR_TIME, seq1(i).OCCUR_TIME)

          if (seq2(0).STATUS == 1)
            seq2.+=:(comm_fac_gk(seq2(0).COMM_FAC_ID, zero, 4))
          if (seq2(seq2.size - 1).STATUS == 4)
            seq2 += comm_fac_gk(seq2(0).COMM_FAC_ID, zero1, 1)
          var lxlxzdsc = 0f
          for (i <- 0 to seq2.size - 1 if (i % 2 == 0)) {
            val sc = minus(seq2(i + 1).OCCUR_TIME, seq2(i).OCCUR_TIME)
            if (sc > lxlxzdsc) lxlxzdsc = sc
          }

          (id.getDecimal(0), zxsc, lxlxzdsc, YZXSC)
        }).toDF.toDF("COMM_FAC_ID", "ZXSC", "LXLXZDSC", "YZXSC").unionAll(fuck).join(PWYW_DMS_TERMINAL_INFO
          .select($"DWBM", $"DWMC", $"DWJB", $"FEEDER_ID", $"FEEDER_NAME", $"TERM_ID", $"TERM_NAME", $"COMM_FAC_ID",
            $"SFNW", $"WHBZ"), Seq("COMM_FAC_ID"))
      .withColumn("ZXL", expr(s"(ZXSC / $YZXSC) * 100")).drop("COMM_FAC_ID").persist

    PDZBToOracle.PWYW_DMS_TMNL_COMM_DET(PWYW_DMS_TMNL_COMM_DET, s"PWYW_DMS_TMNL_COMM_DET$tableName", TJRQ, statType)
    //    PWYW_DMS_TMNL_COMM_DET.show
    //        PWYW_DMS_TMNL_COMM_DET.printSchema()

    val ztUDF = udf((content: String) => content.split("\\s+")(2))

    val PWYW_DMS_YK_DET = hc.read.jdbc(utils.urlSGPMS, s"""(select YK_ID as BRK_ID, OCCUR_TIME as YKSJ, CONTENT from op_yk where occur_time >= 
      to_date('$date1','yyyymmdd') and occur_time < to_date('$date2','yyyymmdd'))""", utils.cpSGPMS)
      .withColumn("ZT", ztUDF($"CONTENT")).filter("ZT in ('控合成功', '控合失败', '控分成功', '控分失败')")
      .withColumn("YKZT", expr("CASE WHEN ZT LIKE '控合%' THEN 1 ELSE 0 END"))
      .withColumn("YKJG", expr("CASE WHEN ZT LIKE '%成功' THEN 1 ELSE 0 END")).drop("ZT")
      .join(PWYW_DMS_BRK_INFO
        .select($"DWBM", $"DWMC", $"DWJB", $"FEEDER_ID", $"FEEDER_NAME", $"TERM_ID", $"TERM_NAME", $"BRK_ID",
          $"BRK_NAME", $"SFNW", $"WHBZ"), Seq("BRK_ID")).persist

    PDZBToOracle.PWYW_DMS_YK_DET(PWYW_DMS_YK_DET, s"PWYW_DMS_YK_DET$tableName", TJRQ, statType)
    //    PWYW_DMS_YK_DET.show
    //        PWYW_DMS_YK_DET.printSchema()

    val zt1UDF = udf((content: String) => content.split("\\s+").last)

    val zt2UDF = udf((content: String) => { val seq = content.split("\\s+"); seq(seq.size - 2) })

    val timeUDF = udf((a: java.sql.Timestamp, b: java.sql.Timestamp) => if (math.abs(a.getTime - b.getTime) < 15 * 60 * 1000) true else false)

    val likeUDF = udf((a: String, b: String) => a.startsWith(b))

    val PWYW_DMS_YX_DET = hc.read.jdbc(utils.urlSGPMS, s"""(select YX_ID as YX_ID1, OCCUR_TIME as YXSJ, CONTENT from yx_bw where occur_time >= 
      to_date('$date1','yyyymmdd') and occur_time < to_date('$date2','yyyymmdd') and status in (1,2,24))""", utils.cpSGPMS)
      .withColumn("YX_DZ", zt1UDF($"CONTENT"))
      .withColumn("YXSJ1", tsToLongUDF($"YXSJ"))
      .join(hc.read.jdbc(utils.urlSGPMS, s"""(select YX_ID AS YX_ID2, OCCUR_TIME as SOE_SJ, CONTENT as SOE_CONTENT, STATUS from yx_soe where occur_time >= 
      to_date('$date1','yyyymmdd') and occur_time < to_date('$date2','yyyymmdd') and STATUS in (0,1))""", utils.cpSGPMS)
        .withColumn("SOE_DZ", expr("CASE WHEN STATUS = 0 THEN '分闸' ELSE '合闸' END"))
        .drop("STATUS").withColumn("SOE_SJ1", tsToLongUDF($"SOE_SJ")),
        $"YX_ID1" === $"YX_ID2" and timeUDF($"YXSJ", $"SOE_SJ") and likeUDF($"YX_DZ", $"SOE_DZ"), "left_outer")
      .withColumn("SOE_DZ", expr("CONCAT(SOE_DZ,'(SOE)')"))
      .withColumn("fuck", row_number.over(Window.partitionBy($"YX_ID1").orderBy(expr("ABS(SOE_SJ1 - YXSJ1)")))) //null -> only yx
      .filter("fuck = 1").drop("YXSJ1").drop("SOE_SJ1").drop("fuck")
      .withColumn("SFPP", expr("CASE WHEN (YX_ID1 IS NOT NULL AND YX_ID2 IS NOT NULL) THEN 1 ELSE 0 END"))
      .join(PWYW_DMS_BRK_INFO
        .select($"DWBM", $"DWMC", $"DWJB", $"FEEDER_ID", $"FEEDER_NAME", $"TERM_ID", $"TERM_NAME", $"BRK_ID", $"BRK_NAME", $"YX_ID", $"SFNW", $"WHBZ"),
        $"YX_ID1" === $"YX_ID").drop("YX_ID").drop("YX_ID1").drop("YX_ID2").persist

    PDZBToOracle.PWYW_DMS_YX_DET(PWYW_DMS_YX_DET, s"PWYW_DMS_YX_DET$tableName", TJRQ, statType)
    //    PWYW_DMS_YX_DET.show
    //        PWYW_DMS_YX_DET.printSchema

    val zxnrUDF = udf((a: String) => if (a.contains("故障处理成功结束")) "故障处理成功结束" else if (a.contains("DA启动分析")) "DA启动分析" else null)

    val PWYW_DMS_KXZDH_DET = hc.read.jdbc(utils.urlSGPMS, s"""(select TRIP_CB as BRK_ID, OCCUR_TIME as ZXSJ, CONTENT from da_process_info where occur_time >= 
      to_date('$date1','yyyymmdd') and occur_time < to_date('$date2','yyyymmdd'))""", utils.cpSGPMS)
      .withColumn("ZXNR", zxnrUDF($"CONTENT")).filter("ZXNR IS NOT NULL")
      .join(PWYW_DMS_BRK_INFO
        .select($"DWBM", $"DWMC", $"DWJB", $"FEEDER_ID", $"FEEDER_NAME", $"TERM_ID", $"TERM_NAME", $"BRK_ID",
          $"BRK_NAME", $"SFNW", $"WHBZ"), Seq("BRK_ID")).persist

    PDZBToOracle.PWYW_DMS_KXZDH_DET(PWYW_DMS_KXZDH_DET, s"PWYW_DMS_KXZDH_DET$tableName", TJRQ, statType)
    //    PWYW_DMS_KXZDH_DET.show
    //        PWYW_DMS_KXZDH_DET.printSchema

    val PWYW_DMS_XLZDH_DET = hc.read.jdbc(utils.url113, """(SELECT DWBM, DWMC, DWJB, FEEDER_ID, FEEDER_NAME, 
      IS_AUTO AS CLJZ, SFNW, WHBZ FROM PWYW.PWYW_DMS_FEEDER_INFO)""", utils.cp)
      .join(PWYW_DMS_TERMINAL_INFO.select($"FEEDER_ID" as "FEEDER_ID1").distinct,
        $"FEEDER_ID1" === $"FEEDER_ID", "left_outer")
      .withColumn("SFFG", expr("CASE WHEN FEEDER_ID1 IS NULL THEN 0 ELSE 1 END")).drop("FEEDER_ID1").persist

    PDZBToOracle.PWYW_DMS_XLZDH_DET(PWYW_DMS_XLZDH_DET, s"PWYW_DMS_XLZDH_DET$tableName", TJRQ, statType)
    //    PWYW_DMS_XLZDH_DET.show
    //        PWYW_DMS_XLZDH_DET.printSchema()

    val PWYW_DMS_XLGZ_DET = hc.read.jdbc(utils.urlSGPMS, s"""(select KEY_ID as YX_ID, OCCUR_TIME as GZSJ, CONTENT from accident_info where occur_time >= 
      to_date('$date1','yyyymmdd') and occur_time < to_date('$date2','yyyymmdd'))""", utils.cpSGPMS)
      .join(PWYW_DMS_BRK_INFO
        .select($"DWBM", $"DWMC", $"DWJB", $"FEEDER_ID", $"FEEDER_NAME", $"TERM_ID", $"TERM_NAME", $"BRK_ID",
          $"BRK_NAME", $"YX_ID", $"SFNW", $"WHBZ"), Seq("YX_ID")).drop("YX_ID").persist

    PDZBToOracle.PWYW_DMS_XLGZ_DET(PWYW_DMS_XLGZ_DET, s"PWYW_DMS_XLGZ_DET$tableName", TJRQ, statType)
    //    PWYW_DMS_XLGZ_DET.show
    //        PWYW_DMS_XLGZ_DET.printSchema

    def statProv = {
      val fun = (df: DataFrame) => {
        df.withColumn("DWBM", lit("232AF1D001B65527E055000000000001"))
      }

      val (n1, n2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 1")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 1")), fun(PWYW_DMS_YX_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 1")), fun(T_SB_ZWYC_XL.filter("SFNW = 1")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 1")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 1")), "DWJB = 3")

      val (c1, c2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 0")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 0")), fun(PWYW_DMS_YX_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 0")), fun(T_SB_ZWYC_XL.filter("SFNW = 0")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 0")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 0")), "DWJB = 3")

      val (q1, q2) = stat(fun(PWYW_DMS_TERMINAL_INFO),
        fun(PWYW_DMS_TMNL_COMM_DET),
        fun(PWYW_DMS_YK_DET), fun(PWYW_DMS_YX_DET),
        fun(PWYW_DMS_KXZDH_DET), fun(T_SB_ZWYC_XL),
        fun(PWYW_DMS_XLZDH_DET), fun(PWYW_DMS_XLGZ_DET), "DWJB = 3")

      val PDZDH_FGL_DF_Q = n2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_N")
        .join(c2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_C"), Seq("DWBM"))
        .withColumn("PDZDH_FGL_DF", expr("PDZDH_FGL_DF_N * 0.4 + PDZDH_FGL_DF_C * 0.6"))

      (n1.withColumn("CNW", lit(3)).unionAll(c1.withColumn("CNW", lit(2))).unionAll(q1.withColumn("CNW", lit(1))),
        n2.withColumn("CNW", lit(3)).unionAll(c2.withColumn("CNW", lit(2)))
        .unionAll(q2.drop("PDZDH_FGL_DF").drop("JSGLZB_DF").join(PDZDH_FGL_DF_Q, Seq("DWBM"))
          .withColumn("JSGLZB_DF", expr("PDZDH_FGL_DF + KXZDH_XLFGL_DF + GZZDPD_CLL_DF + 30"))
          .select("DWBM", "DWMC", "DWJB", "XLZS", "PDZDH_XLZS", "PDZDH_FGS", "KXZDH_GNPZ_XLZS", "PDXL_GZZS", "KXZDH_QDZS",
            "GZZDPD_CLL", "GZZDPD_CLL_DF", "PDZDH_FGL", "PDZDH_FGL_DF", "KXZDH_XLFGL", "KXZDH_XLFGL_DF", "JSGLZB_DF") //!!!
          .withColumn("CNW", lit(1))))
    }

    def statCity = {
      val fun = (df: DataFrame) => {
        df.join(city, $"CITY" === $"DWBM").withColumn("DWBM", $"CITY").drop("CITY").unionAll(
          df.join(townOfCity, $"TOWN" === $"DWBM").withColumn("DWBM", $"CITY").drop("CITY").drop("TOWN")).unionAll(
            df.join(bzOfCity, $"BZ" === $"DWBM").withColumn("DWBM", $"CITY").drop("CITY").drop("BZ"))
      }

      val (n1, n2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 1")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 1")), fun(PWYW_DMS_YX_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 1")), fun(T_SB_ZWYC_XL.filter("SFNW = 1")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 1")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 1")), "DWJB = 4")

      val (c1, c2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 0")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 0")), fun(PWYW_DMS_YX_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 0")), fun(T_SB_ZWYC_XL.filter("SFNW = 0")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 0")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 0")), "DWJB = 4")

      val (q1, q2) = stat(fun(PWYW_DMS_TERMINAL_INFO),
        fun(PWYW_DMS_TMNL_COMM_DET),
        fun(PWYW_DMS_YK_DET), fun(PWYW_DMS_YX_DET),
        fun(PWYW_DMS_KXZDH_DET), fun(T_SB_ZWYC_XL),
        fun(PWYW_DMS_XLZDH_DET), fun(PWYW_DMS_XLGZ_DET), "DWJB = 4")

      val PDZDH_FGL_DF_Q = n2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_N")
        .join(c2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_C"), Seq("DWBM"))
        .withColumn("PDZDH_FGL_DF", expr("PDZDH_FGL_DF_N * 0.4 + PDZDH_FGL_DF_C * 0.6"))

      (n1.withColumn("CNW", lit(3)).unionAll(c1.withColumn("CNW", lit(2))).unionAll(q1.withColumn("CNW", lit(1))),
        n2.withColumn("CNW", lit(3)).unionAll(c2.withColumn("CNW", lit(2)))
        .unionAll(q2.drop("PDZDH_FGL_DF").drop("JSGLZB_DF").join(PDZDH_FGL_DF_Q, Seq("DWBM"))
          .withColumn("JSGLZB_DF", expr("PDZDH_FGL_DF + KXZDH_XLFGL_DF + GZZDPD_CLL_DF + 30"))
          .select("DWBM", "DWMC", "DWJB", "XLZS", "PDZDH_XLZS", "PDZDH_FGS", "KXZDH_GNPZ_XLZS", "PDXL_GZZS", "KXZDH_QDZS",
            "GZZDPD_CLL", "GZZDPD_CLL_DF", "PDZDH_FGL", "PDZDH_FGL_DF", "KXZDH_XLFGL", "KXZDH_XLFGL_DF", "JSGLZB_DF") //!!!
          .withColumn("CNW", lit(1))))
    }

    def statTown = {
      val fun = (df: DataFrame) => {
        df.join(town, $"TOWN" === $"DWBM").withColumn("DWBM", $"TOWN").drop("TOWN").unionAll(
          df.join(bzOfTown, $"BZ" === $"DWBM").withColumn("DWBM", $"TOWN").drop("TOWN").drop("BZ"))
      }

      val (n1, n2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 1")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 1")), fun(PWYW_DMS_YX_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 1")), fun(T_SB_ZWYC_XL.filter("SFNW = 1")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 1")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 1")), "DWJB = 5")

      val (c1, c2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 0")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 0")), fun(PWYW_DMS_YX_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 0")), fun(T_SB_ZWYC_XL.filter("SFNW = 0")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 0")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 0")), "DWJB = 5")

      val (q1, q2) = stat(fun(PWYW_DMS_TERMINAL_INFO),
        fun(PWYW_DMS_TMNL_COMM_DET),
        fun(PWYW_DMS_YK_DET), fun(PWYW_DMS_YX_DET),
        fun(PWYW_DMS_KXZDH_DET), fun(T_SB_ZWYC_XL),
        fun(PWYW_DMS_XLZDH_DET), fun(PWYW_DMS_XLGZ_DET), "DWJB = 5")

      val PDZDH_FGL_DF_Q = n2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_N")
        .join(c2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_C"), Seq("DWBM"))
        .withColumn("PDZDH_FGL_DF", expr("PDZDH_FGL_DF_N * 0.4 + PDZDH_FGL_DF_C * 0.6"))

      (n1.withColumn("CNW", lit(3)).unionAll(c1.withColumn("CNW", lit(2))).unionAll(q1.withColumn("CNW", lit(1))),
        n2.withColumn("CNW", lit(3)).unionAll(c2.withColumn("CNW", lit(2)))
        .unionAll(q2.drop("PDZDH_FGL_DF").drop("JSGLZB_DF").join(PDZDH_FGL_DF_Q, Seq("DWBM"))
          .withColumn("JSGLZB_DF", expr("PDZDH_FGL_DF + KXZDH_XLFGL_DF + GZZDPD_CLL_DF + 30"))
          .select("DWBM", "DWMC", "DWJB", "XLZS", "PDZDH_XLZS", "PDZDH_FGS", "KXZDH_GNPZ_XLZS", "PDXL_GZZS", "KXZDH_QDZS",
            "GZZDPD_CLL", "GZZDPD_CLL_DF", "PDZDH_FGL", "PDZDH_FGL_DF", "KXZDH_XLFGL", "KXZDH_XLFGL_DF", "JSGLZB_DF") //!!!
          .withColumn("CNW", lit(1))))
    }
    
    def statBZ = {
      val fun = (df: DataFrame) => {
        df.withColumn("DWBM", $"WHBZ")
      }

      val (n1, n2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 1")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 1")), fun(PWYW_DMS_YX_DET.filter("SFNW = 1")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 1")), fun(T_SB_ZWYC_XL.filter("SFNW = 1")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 1")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 1")), "DWJB = 8")

      val (c1, c2) = stat(fun(PWYW_DMS_TERMINAL_INFO.filter("SFNW = 0")),
        fun(PWYW_DMS_TMNL_COMM_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_YK_DET.filter("SFNW = 0")), fun(PWYW_DMS_YX_DET.filter("SFNW = 0")),
        fun(PWYW_DMS_KXZDH_DET.filter("SFNW = 0")), fun(T_SB_ZWYC_XL.filter("SFNW = 0")),
        fun(PWYW_DMS_XLZDH_DET.filter("SFNW = 0")), fun(PWYW_DMS_XLGZ_DET.filter("SFNW = 0")), "DWJB = 8")

      val (q1, q2) = stat(fun(PWYW_DMS_TERMINAL_INFO),
        fun(PWYW_DMS_TMNL_COMM_DET),
        fun(PWYW_DMS_YK_DET), fun(PWYW_DMS_YX_DET),
        fun(PWYW_DMS_KXZDH_DET), fun(T_SB_ZWYC_XL),
        fun(PWYW_DMS_XLZDH_DET), fun(PWYW_DMS_XLGZ_DET), "DWJB = 8")

      val PDZDH_FGL_DF_Q = n2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_N")
        .join(c2.select($"DWBM", $"PDZDH_FGL_DF" as "PDZDH_FGL_DF_C"), Seq("DWBM"))
        .withColumn("PDZDH_FGL_DF", expr("PDZDH_FGL_DF_N * 0.4 + PDZDH_FGL_DF_C * 0.6"))

      (n1.withColumn("CNW", lit(3)).unionAll(c1.withColumn("CNW", lit(2))).unionAll(q1.withColumn("CNW", lit(1))),
        n2.withColumn("CNW", lit(3)).unionAll(c2.withColumn("CNW", lit(2)))
        .unionAll(q2.drop("PDZDH_FGL_DF").drop("JSGLZB_DF").join(PDZDH_FGL_DF_Q, Seq("DWBM"))
          .withColumn("JSGLZB_DF", expr("PDZDH_FGL_DF + KXZDH_XLFGL_DF + GZZDPD_CLL_DF + 30"))
          .select("DWBM", "DWMC", "DWJB", "XLZS", "PDZDH_XLZS", "PDZDH_FGS", "KXZDH_GNPZ_XLZS", "PDXL_GZZS", "KXZDH_QDZS",
            "GZZDPD_CLL", "GZZDPD_CLL_DF", "PDZDH_FGL", "PDZDH_FGL_DF", "KXZDH_XLFGL", "KXZDH_XLFGL_DF", "JSGLZB_DF") //!!!
          .withColumn("CNW", lit(1))))
    }

    def stat(PWYW_DMS_TERMINAL_INFO: DataFrame, PWYW_DMS_TMNL_COMM_DET: DataFrame, PWYW_DMS_YK_DET: DataFrame,
             PWYW_DMS_YX_DET: DataFrame, PWYW_DMS_KXZDH_DET: DataFrame, T_SB_ZWYC_XL: DataFrame,
             PWYW_DMS_XLZDH_DET: DataFrame, PWYW_DMS_XLGZ_DET: DataFrame, filter: String) = {
      val a = PWYW_DMS_TERMINAL_INFO.select($"DWBM", $"TERM_ID").groupBy($"DWBM")
        .agg(count($"TERM_ID") as "ZD_ZS")

      val b = PWYW_DMS_TMNL_COMM_DET.groupBy($"DWBM").agg(sum($"ZXSC") as "ZD_ZXSC", sum($"YZXSC") as "ZD_YZXSC",
        sum(expr("CASE WHEN LXLXZDSC <= 72 THEN 1 ELSE 0 END")) as "ZD_LXDSZS", avg($"ZXL") as "ZD_PJZXL")

      val c = PWYW_DMS_YK_DET.groupBy($"DWBM").agg(count($"YKZT") as "YKCZ_ZCS",
        sum($"YKJG") as "YKCZ_CGCS").withColumn("YKCZ_SBCS", $"YKCZ_ZCS" - $"YKCZ_CGCS")

      val d = PWYW_DMS_YX_DET.groupBy($"DWBM").agg(count($"CONTENT") as "YX_BWZS", sum($"SFPP") as "YX_SOE_PPZS")

      val e = PWYW_DMS_KXZDH_DET.groupBy($"DWBM").agg(
        sum(expr("CASE WHEN ZXNR = 'DA启动分析' THEN 1 ELSE 0 END")) as "KXZDH_QDZS",
        sum(expr("CASE WHEN ZXNR = '故障处理成功结束' THEN 1 ELSE 0 END")) as "KXZDH_CGZXZS")
        .persist

      val PWYW_DMS_YXZB = a.join(b, Seq("DWBM"), "left_outer")
        .join(c, Seq("DWBM"), "left_outer").join(d, Seq("DWBM"), "left_outer")
        .join(e, Seq("DWBM"), "left_outer")

      val f = T_SB_ZWYC_XL.groupBy($"DWBM").agg(count($"OBJ_ID") as "XLZS")

      val g = PWYW_DMS_XLZDH_DET.groupBy($"DWBM").agg(count($"FEEDER_ID") as "PDZDH_XLZS",
        sum($"SFFG") cast "int" as "PDZDH_FGS", sum($"CLJZ") cast "int" as "KXZDH_GNPZ_XLZS")

      val h = PWYW_DMS_XLGZ_DET.groupBy($"DWBM").agg(count($"CONTENT") as "PDXL_GZZS")
        .join(e.select("DWBM", "KXZDH_QDZS"), Seq("DWBM"), "left_outer")

      val PWYW_DMS_JSGLZB = f.join(g, Seq("DWBM"), "left_outer")
        .join(h, Seq("DWBM"), "left_outer")

      (DW_ARCH.filter(filter).join(PWYW_DMS_YXZB, Seq("DWBM"), "left_outer").na.fill(0)
        .withColumn("YKCZ_CGL", expr("CASE WHEN YKCZ_ZCS = 0 THEN 100 ELSE YKCZ_CGCS / YKCZ_ZCS * 100 END"))
        .withColumn("YKCZ_CGL_DF", expr("YKCZ_CGL / 4"))
        .withColumn("YX_ZQL", expr("CASE WHEN YX_BWZS = 0 THEN 100 ELSE YX_SOE_PPZS / YX_BWZS * 100 END"))
        .withColumn("YX_ZQL_DF", expr("YX_ZQL / 5"))
        .withColumn("KXZDH_CGL", expr("CASE WHEN KXZDH_QDZS = 0 THEN 100 ELSE KXZDH_CGZXZS / KXZDH_QDZS * 100 END"))
        .withColumn("KXZDH_CGL_DF", expr("KXZDH_CGL / 100 * 30"))
        .withColumn("ZD_PJZXL", expr("CASE WHEN ZD_PJZXL = 0 THEN 100 ELSE ZD_PJZXL END"))
        .withColumn("ZD_PJZXL_DF", expr("""CASE WHEN ZD_PJZXL >= 95 THEN 25 ELSE 25 * (0.5 * (ZD_ZXSC / ZD_YZXSC) + 
            0.5 * (ZD_LXDSZS / ZD_ZS)) END"""))
        .withColumn("PDZDH_YXZB_DF", expr("ZD_PJZXL_DF + YKCZ_CGL_DF + YX_ZQL_DF + KXZDH_CGL_DF")).persist,
        DW_ARCH.filter(filter).join(PWYW_DMS_JSGLZB, Seq("DWBM"), "left_outer").na.fill(0)
        .withColumn("GZZDPD_CLL", expr("CASE WHEN PDXL_GZZS = 0 THEN 100 ELSE KXZDH_QDZS / PDXL_GZZS * 100 END"))
        .withColumn("GZZDPD_CLL_DF", expr("GZZDPD_CLL / 4"))
        .withColumn("PDZDH_FGL", expr("CASE WHEN XLZS = 0 THEN 100 ELSE PDZDH_FGS / XLZS * 100 END"))
        .withColumn("PDZDH_FGL_DF", expr("PDZDH_FGL / 4"))
        .withColumn("KXZDH_XLFGL", expr("CASE WHEN PDZDH_FGS = 0 THEN 100 ELSE KXZDH_GNPZ_XLZS / PDZDH_FGS * 100 END"))
        .withColumn("KXZDH_XLFGL_DF", expr("KXZDH_XLFGL / 5"))
        .withColumn("JSGLZB_DF", expr("PDZDH_FGL_DF + KXZDH_XLFGL_DF + GZZDPD_CLL_DF + 30")).persist)

    }
    val prov1 = statProv
    val city1 = statCity
    val town1 = statTown
    val bz = statBZ

    //    prov1._1.printSchema()
    //    prov1._2.printSchema()

    PDZBToOracle.PWYW_DMS_YXZB(prov1._1.unionAll(city1._1).unionAll(town1._1).unionAll(bz._1), s"PWYW_DMS_YXZB$tableName", TJRQ, statType)
    PDZBToOracle.PWYW_DMS_JSGLZB(prov1._2.unionAll(city1._2).unionAll(town1._2).unionAll(bz._2), s"PWYW_DMS_JSGLZB$tableName", TJRQ, statType)

  }
}