package spark.util

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Row

import com.nari.properties.CommProperties
import java.sql.DriverManager
import java.sql.Connection

object OracleUtils {
  def buildSchema3(): Unit = {
    val str = """OracleUtils.ExecPS(data, ps, index, "GZYY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SSTDCID", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "OBJ_ID", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SSGDDW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "LY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFDWGZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFDDXZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFQRGZD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFBHZYYH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFDX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BXR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BXSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "BXNR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "LXR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "LXDZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "LXDH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YHBH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YHMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YHLX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YHSJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SSQX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "PPHHLX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DJDW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DJBM", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DJR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SJDYMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DYDJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SYZX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZXLX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZSBBH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZSBMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZSBLX_BM", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "TZSBBH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "TZSBMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CLXCFL", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZDD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZMS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "TZQK", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "TZSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "TZYY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZDZCZW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZWHCD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZLX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZJB", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YJFL", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "EJFL", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SJFL", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SIJFL", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZXX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JJCD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZSBCQ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZYYMS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "XFCSYY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "TQ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CLJG", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CLJGMS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JDDJSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "TZKCSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "JLDDSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "HBSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "TZQXSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "DWGLSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "GZXKSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "GZHBSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "JLXFSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "HFSDSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "ZZXFSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "ZFDDSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "DDHTSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "YJXFSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "CNDDSJ", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "BFFHZYSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "QBFHZYSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "TDZSC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "GDYJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GDRY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GDSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "SFGD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZFYJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZFRY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZFSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "SFZF", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "HTYJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "HTSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "HTRY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SBSSBM", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SBYXRY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YXDW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CLDW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXZDH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "HXYWDH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "HXYWDLX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXDZT", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZDJD", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "GZDWD", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "SFCS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CSSM", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZLB", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SYZXMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "TZSBLX_BM", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFTZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DJRMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CLDWMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "FXR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXDBH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SCGXSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "SFZXGZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SQDBH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JXGD_ID", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFDYJDGZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "WJFL", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "XZDW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "AQCSSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "BBHSBID", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BBHSBMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BBHSBLX", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BHDZQK", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BHDZSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "QCFH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SSFH", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZJHM", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "KHYJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZXCLR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ZXCLRMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JPDRY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JPDRYMC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "LXRWZT", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "XCJDSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "TDXXBH", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "TDTZDB_ID", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SSBMBZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SQDZT", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "BZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "HTYY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "FIRSTCFALG", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CONTACTNAME", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CONTACTWAY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "CONTACTTIME", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "NCONTREASON", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DEALRESULT", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "ISPOWERDUTY", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFZDJD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZDJD_INTERNET", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "GZDWD_INTERNET", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "SLYJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YSGDDW", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXZHBZ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZFSSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "RKSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "SFZP", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JDCGSJ", java.sql.Types.TIMESTAMP); index += 1
OracleUtils.ExecPS(data, ps, index, "DDXCSC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "JDSC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "GDCLSC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "QXSC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "QXSC_QXC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "QXSC_YHC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "PDSC", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "GZNR", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "YXYHS", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "YXZBSL", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "YXGBSL", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "DQZT", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXDW_ID", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXDW_FZR_MC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXDW_FZR_LXFS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "QXDW_MC", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "DQHJ", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFJDCS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "SFDDCS", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "JDFS", java.sql.Types.NUMERIC); index += 1
OracleUtils.ExecPS(data, ps, index, "SFWFD", java.sql.Types.VARCHAR); index += 1
OracleUtils.ExecPS(data, ps, index, "GZYYFL", java.sql.Types.VARCHAR); index += 1
"""

    val str1 = str.replaceAll("\\|--", "").replaceAll("\\(nullable = false\\)", "").replaceAll("\\(nullable = true\\)", "").replaceAll(" ", "")
    val result = str1.split(":").mkString(" ")
    val arry = result.split("\n")
    val result1 = arry.zipWithIndex.map {
      r =>
        {
          val row = r._1.split(" ")
          if (row(1) == "string")
            s"""OracleUtils.ExecPS(data, ps, index, "${row(0)}", java.sql.Types.VARCHAR); index += 1"""
          else if (row(1) == "timestamp")
            s"""OracleUtils.ExecPS(data, ps, index, "${row(0)}", java.sql.Types.TIMESTAMP); index += 1"""
          else
            s"""OracleUtils.ExecPS(data, ps, index, "${row(0)}", java.sql.Types.NUMERIC); index += 1"""
        }
    }
    println(arry.zipWithIndex.map(r => r._2 + " " + r._1).mkString("\n"))
    println(result1.mkString("\n"))
    println(arry.size)
    println((1 to arry.size).map(_ => "?").mkString(","))
    println(arry.map(_.replaceAll(" string", "").replaceAll(" float", "").replaceAll(" integer", "").replaceAll(" timestamp", "").replaceAll(" decimal(\\S+)", "").replaceAll(" long", "").replaceAll(" double", "")).mkString(","))
  }

  def StringToDate(s: Any): java.sql.Date = {
    val sdf = new SimpleDateFormat(CommProperties.FORMAT_DATE_DAY)
    try {
      val date = s.asInstanceOf[String]
      new java.sql.Date(sdf.parse(date).getTime)
    } catch {
      case e: Exception => null
    }
  }

  def StringToTimeStamp(s: Any): java.sql.Timestamp = {
   // val sdf = new SimpleDateFormat(CommProperties.FORMAT_TIMESTAMP)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    try {
      val date = s.asInstanceOf[String]
      new java.sql.Timestamp(sdf.parse(date).getTime)
    } catch {
      case e: Exception => null
    }
  }

  def ExecPS(row: Row, ps: PreparedStatement, index: Int, filedName: String, sqlType: Int): Unit = {
    val value = row.getAs[Any](filedName)
    val ind = row.fieldIndex(filedName)
    if (row.isNullAt(ind))
      ps.setNull(index, sqlType)
    else {
      if (value.isInstanceOf[String] && sqlType == 91) {
        if (StringToDate(value) != null) ps.setDate(index, StringToDate(value))
        else ps.setNull(index, sqlType)
      } else if (value.isInstanceOf[String] && sqlType == 93) {
        if (StringToTimeStamp(value) != null) ps.setTimestamp(index, StringToTimeStamp(value))
        else ps.setNull(index, sqlType)
      } else {
        value match {
          case value: java.lang.Long       => ps.setLong(index, value.asInstanceOf[java.lang.Long])
          case value: java.lang.Double     => if (value.isNaN()) ps.setNull(index, sqlType) else ps.setDouble(index, value.asInstanceOf[java.lang.Double])
          case value: java.lang.Integer    => ps.setInt(index, value.asInstanceOf[Int])
          case value: java.lang.Float      => if (value.isNaN()) ps.setNull(index, sqlType) else ps.setFloat(index, value.asInstanceOf[java.lang.Float])
          case value: java.math.BigDecimal => ps.setBigDecimal(index, value.asInstanceOf[java.math.BigDecimal])
          case value: java.sql.Date        => ps.setDate(index, value.asInstanceOf[java.sql.Date])
          case value: java.sql.Timestamp   => ps.setTimestamp(index, value.asInstanceOf[java.sql.Timestamp])
          case value: String               => ps.setString(index, value.asInstanceOf[String])
        }
      }
    }
  }
  
  def ExecPS(row: Row, index: Int, ps: PreparedStatement, sqlType: Int) {
    val index1 = index - 1
    val a = row.get(index1)
    if (row.isNullAt(index1))
      ps.setNull(index, sqlType)
    else {
      a match {
        case a: java.lang.Long    => ps.setLong(index, a.asInstanceOf[java.lang.Long])
        case a: java.lang.Double    => ps.setDouble(index, a.asInstanceOf[java.lang.Double])
        case a: java.lang.Integer    => ps.setInt(index, a.asInstanceOf[Int])
        case a: java.lang.Float      => ps.setFloat(index, a.asInstanceOf[java.lang.Float])
        case a: java.math.BigDecimal => ps.setBigDecimal(index, a.asInstanceOf[java.math.BigDecimal])
        case a: java.sql.Date        => ps.setDate(index, a.asInstanceOf[java.sql.Date])
        case a: java.sql.Timestamp        => ps.setTimestamp(index, a.asInstanceOf[java.sql.Timestamp])
        case a: String               => ps.setString(index, a.asInstanceOf[String])
      }
    }
  }
}