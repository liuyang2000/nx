package spark.task.NX

import java.sql.DriverManager
import spark.util.utils
import org.apache.spark.sql.DataFrame
import spark.util.OracleUtils
import java.sql.PreparedStatement
import java.sql.Connection
import spark.util.OracleUtils

object NXToOracle {
  def qxdDetail(data: DataFrame, statDate: String, statType: String) {
    val map = Map("DAY" -> 1, "WEEK" -> 2, "MONTH" -> 3, "SEASON" -> 4, "HY" -> 5, "YEAR" -> 6)
    val statTypeInt = map(statType)

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.PWYW_T_YJ_GZQX_QXD_DET where TJRQ = to_date('$statDate','yyyyMMdd') and TJRQWD = $statTypeInt """
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.PWYW_T_YJ_GZQX_QXD_DET (BZSSGDDW,GZYY,SSTDCID,OBJ_ID,SSGDDW,LY,SFDWGZ,SFDDXZ,SFQRGZD,SFBHZYYH,SFDX,BXR,BXSJ,BXNR,LXR,LXDZ,LXDH,YHBH,
        YHMC,YHLX,YHSJ,SSQX,PPHHLX,DJDW,DJBM,DJR,SJDYMC,DYDJ,SYZX,ZXLX,GZSBBH,GZSBMC,GZSBLX_BM,TZSBBH,TZSBMC,CLXCFL,GZDD,GZMS,TZQK,TZSJ,TZYY,GZDZCZW,GZWHCD,
        GZLX,GZJB,YJFL,EJFL,SJFL,SIJFL,GZXX,JJCD,GZSBCQ,GZYYMS,XFCSYY,TQ,CLJG,CLJGMS,JDDJSJ,TZKCSJ,JLDDSJ,HBSJ,TZQXSJ,DWGLSJ,GZXKSJ,GZHBSJ,JLXFSJ,HFSDSJ,
        ZZXFSJ,ZFDDSJ,DDHTSJ,YJXFSJ,CNDDSJ,BFFHZYSJ,QBFHZYSJ,TDZSC,GDYJ,GDRY,GDSJ,SFGD,ZFYJ,ZFRY,ZFSJ,SFZF,HTYJ,HTSJ,HTRY,SBSSBM,SBYXRY,YXDW,CLDW,QXZDH,
        HXYWDH,HXYWDLX,QXDZT,GZDJD,GZDWD,SFCS,CSSM,GZLB,SYZXMC,TZSBLX_BM,SFTZ,DJRMC,CLDWMC,FXR,QXDBH,SCGXSJ,SFZXGZ,SQDBH,JXGD_ID,SFDYJDGZ,WJFL,XZDW,AQCSSJ,
        BBHSBID,BBHSBMC,BBHSBLX,BHDZQK,BHDZSJ,QCFH,SSFH,ZJHM,KHYJ,ZXCLR,ZXCLRMC,JPDRY,JPDRYMC,LXRWZT,XCJDSJ,TDXXBH,TDTZDB_ID,SSBMBZ,SQDZT,BZ,HTYY,FIRSTCFALG,
        CONTACTNAME,CONTACTWAY,CONTACTTIME,NCONTREASON,DEALRESULT,ISPOWERDUTY,SFZDJD,GZDJD_INTERNET,GZDWD_INTERNET,SLYJ,YSGDDW,QXZHBZ,GZFSSJ,RKSJ,SFZP,JDCGSJ,
        DDXCSC,JDSC,GDCLSC,QXSC,QXSC_QXC,QXSC_YHC,PDSC,GZNR,YXYHS,YXZBSL,YXGBSL,DQZT,QXDW_ID,QXDW_FZR_MC,QXDW_FZR_LXFS,QXDW_MC,DQHJ,SFJDCS,SFDDCS,JDFS,SFWFD,GZYYFL,TJRQWD,TJRQ)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,$statTypeInt,to_date('$statDate','yyyyMMdd'))"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        //        conn = DriverManager.getConnection(utils.urltest, utils.cptest.getProperty("user"), utils.cptest.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          var index = 1
          OracleUtils.ExecPS(data, ps, index, "BZSSGDDW", java.sql.Types.VARCHAR); index += 1
          OracleUtils.ExecPS(data, ps, index, "GZYY", java.sql.Types.VARCHAR); index += 1
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

          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def gdDetail(data: DataFrame, statDate: String, statType: String) {
    val map = Map("DAY" -> 1, "WEEK" -> 2, "MONTH" -> 3, "SEASON" -> 4, "HY" -> 5, "YEAR" -> 6)
    val statTypeInt = map(statType)

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.PWYW_T_YJ_GZQX_QXD where TJRQ = to_date('$statDate','yyyyMMdd') and TJRQWD = $statTypeInt """
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.PWYW_T_YJ_GZQX_QXD (OBJ_ID,SSGDDW,LY,SFDWGZ,SFDDXZ,SFQRGZD,SFBHZYYH,SFDX,BXR,BXSJ,BXNR,LXR,LXDZ,LXDH,YHBH,YHMC,YHLX,
	        YHSJ,SSQX,PPHHLX,DJDW,DJBM,DJR,SJDYMC,DYDJ,SYZX,ZXLX,GZSBBH,GZSBMC,GZSBLX_BM,TZSBBH,TZSBMC,CLXCFL,GZDD,GZMS,TZQK,TZSJ,TZYY,GZDZCZW,GZWHCD,GZLX,
	        GZJB,YJFL,EJFL,SJFL,SIJFL,GZXX,JJCD,GZSBCQ,GZYY,GZYYMS,XFCSYY,TQ,CLJG,CLJGMS,JDDJSJ,TZKCSJ,JLDDSJ,HBSJ,TZQXSJ,DWGLSJ,GZXKSJ,GZHBSJ,JLXFSJ,HFSDSJ,
	        ZZXFSJ,ZFDDSJ,DDHTSJ,YJXFSJ,CNDDSJ,BFFHZYSJ,QBFHZYSJ,TDZSC,GDYJ,GDRY,GDSJ,SFGD,ZFYJ,ZFRY,ZFSJ,SFZF,HTYJ,HTSJ,HTRY,SBSSBM,SBYXRY,YXDW,CLDW,QXZDH,
	        HXYWDH,HXYWDLX,QXDZT,GZDJD,GZDWD,SFCS,CSSM,GZLB,SYZXMC,TZSBLX_BM,SFTZ,DJRMC,CLDWMC,FXR,QXDBH,SCGXSJ,SFZXGZ,SQDBH,JXGD_ID,SFDYJDGZ,WJFL,XZDW,AQCSSJ,
	        BBHSBID,BBHSBMC,BBHSBLX,BHDZQK,BHDZSJ,QCFH,SSFH,ZJHM,KHYJ,ZXCLR,ZXCLRMC,JPDRY,JPDRYMC,LXRWZT,XCJDSJ,TDXXBH,TDTZDB_ID,SSBMBZ,SQDZT,BZ,HTYY,
	        FIRSTCFALG,CONTACTNAME,CONTACTWAY,CONTACTTIME,NCONTREASON,DEALRESULT,ISPOWERDUTY,SFZDJD,GZDJD_INTERNET,GZDWD_INTERNET,SLYJ,YSGDDW,QXZHBZ,
	        GZFSSJ,RKSJ,SSTDCID,SFZP,JDCGSJ,DDXCSC,JDSC,GDCLSC,QXSC,TJRQWD,TJRQ) 
	        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
	        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
	        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,$statTypeInt,to_date('$statDate','yyyyMMdd'))"""
      //        log errors into pwyw.ERR_PWYW_T_YJ_GZQX_QXD('dfd')"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 18, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 19, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 20, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 21, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 22, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 23, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 24, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 25, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 26, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 27, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 28, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 29, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 30, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 31, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 32, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 33, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 34, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 35, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 36, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 37, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 38, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 39, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 40, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 41, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 42, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 43, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 44, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 45, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 46, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 47, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 48, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 49, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 50, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 51, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 52, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 53, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 54, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 55, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 56, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 57, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 58, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 59, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 60, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 61, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 62, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 63, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 64, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 65, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 66, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 67, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 68, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 69, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 70, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 71, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 72, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 73, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 74, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 75, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 76, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 77, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 78, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 79, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 80, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 81, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 82, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 83, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 84, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 85, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 86, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 87, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 88, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 89, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 90, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 91, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 92, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 93, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 94, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 95, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 96, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 97, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 98, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 99, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 100, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 101, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 102, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 103, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 104, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 105, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 106, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 107, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 108, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 109, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 110, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 111, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 112, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 113, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 114, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 115, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 116, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 117, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 118, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 119, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 120, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 121, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 122, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 123, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 124, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 125, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 126, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 127, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 128, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 129, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 130, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 131, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 132, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 133, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 134, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 135, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 136, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 137, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 138, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 139, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 140, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 141, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 142, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 143, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 144, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 145, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 146, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 147, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 148, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 149, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 150, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 151, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 152, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 153, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 154, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 155, ps, java.sql.Types.NUMERIC)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def gdtj(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (DWBM,GDLY,YXFW,GDZS,DBZS,YBZS,ZZCLZS,PJQXSC,DDCSGDS,JDCSGDS,DWMC,DWJB,SJDWBM,SJDWMC,GDLX,TJRQ) 
            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" + (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def gdtjW(data: DataFrame, tableName: String, statDateS: String, statDateE: String) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDateS','yyyyMMdd')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (DWBM,GDLY,YXFW,GDZS,DBZS,YBZS,ZZCLZS,PJQXSC,DDCSGDS,JDCSGDS,DWMC,DWJB,SJDWBM,SJDWMC,GDLX,TJKSRQ,TJJSRQ) 
            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDateS','yyyyMMdd'),to_date('$statDateE','yyyyMMdd'))"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }

  def tydb(data: DataFrame, tableName: String, statDate: String, isDay: Boolean) {
    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = """ + (if (isDay) s"to_date('$statDate','yyyyMMdd')" else s"'$statDate'")
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (DWBM,GBZRL,DYYHZS,DYYHHJPBRL,PDXLGDBJH,PDXLZTS,PDXLPJGDBJ,DYYHJCGYPBSL,GBZSL,DYYHJCDFGL,YDYGBZS,GBZS,
        GBYPDYYZL,WCYXSJCJGBZS,GBYXSJCJL,SJCJSJLDS,YCJSJLZDS,GBYXSJWZL,JKXLLLKGZS,JKXLFDKGZS,DKXTS,JKXLWJHGL,FZYCGBZS,ZZGBZS,GZGBZS,GBFZHGL,SXBPHYCGBTS,
        GBSXBPHHLL,GDYGBTS,DDYGBTS,YCDYGBTS,GBDYHLL,CNW,DWMC,SJ_DWBM,SJ_DWMC,GBYXSJHLL,DWJB,TJRQ)
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,""" +
        (if (isDay) s"to_date('$statDate','yyyyMMdd'))" else s"'$statDate')")
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 18, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 19, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 20, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 21, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 22, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 23, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 24, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 25, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 26, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 27, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 28, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 29, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 30, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 31, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 32, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 33, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 34, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 35, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 36, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 37, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 38, ps, java.sql.Types.VARCHAR)
          ps.addBatch()

          if (iCnt % 10000 == 0 && iCnt != 0) {
            println(s"============================$iCnt")
            ps.executeBatch()
            ps.clearBatch()
            conn.commit()
          }
        })

        println(s"============================$iCnt")
        ps.executeBatch()
        ps.clearBatch()
        conn.commit()

      } finally {
        if (ps != null) {
          ps.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    })
  }
}