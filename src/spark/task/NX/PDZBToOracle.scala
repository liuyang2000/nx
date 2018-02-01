package spark.task.NX

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement

import org.apache.spark.sql.DataFrame

import spark.util.OracleUtils
import spark.util.utils
import java.util.Calendar

object PDZBToOracle {

  def PWYW_DMS_TMNL_COMM_DET(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val dateFormat = statType match {
      case "DAY"    => "yyyyMMdd"
      case "WEEK"   => "yyyyMMdd"
      case "MONTH"  => "yyyyMM"
      case "SEASON" => "yyyyMM"
      case "HY"     => "yyyyMM"
      case "YEAR"   => "yyyy"
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','$dateFormat')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (ZXSC,LXLXZDSC,YZXSC,DWBM,DWMC,DWJB,FEEDER_ID,FEEDER_NAME,TERM_ID,TERM_NAME,SFNW,WHBZ,ZXL,TJRQ) 
        values(?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,to_date('$statDate','$dateFormat'))"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.VARCHAR)
          
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.VARCHAR)
          
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          
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

  def PWYW_DMS_YK_DET(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val dateFormat = statType match {
      case "DAY"    => "yyyyMMdd"
      case "WEEK"   => "yyyyMMdd"
      case "MONTH"  => "yyyyMM"
      case "SEASON" => "yyyyMM"
      case "HY"     => "yyyyMM"
      case "YEAR"   => "yyyy"
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','$dateFormat')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (BRK_ID,YKSJ,CONTENT,YKZT,YKJG,DWBM,DWMC,DWJB,FEEDER_ID,FEEDER_NAME,TERM_ID,TERM_NAME,BRK_NAME,SFNW,TJRQ,WHBZ) 
        values(?,?,?,?,?,
               ?,?,?,?,?,
               ?,?,?,?,to_date('$statDate','$dateFormat'),
               ?)"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
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

  def PWYW_DMS_YX_DET(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val dateFormat = statType match {
      case "DAY"    => "yyyyMMdd"
      case "WEEK"   => "yyyyMMdd"
      case "MONTH"  => "yyyyMM"
      case "SEASON" => "yyyyMM"
      case "HY"     => "yyyyMM"
      case "YEAR"   => "yyyy"
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','$dateFormat')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (YXSJ,CONTENT,YX_DZ,SOE_SJ,SOE_CONTENT,
        SOE_DZ,SFPP,DWBM,DWMC,DWJB,
        FEEDER_ID,FEEDER_NAME,TERM_ID,TERM_NAME,BRK_ID,
        BRK_NAME,SFNW,TJRQ,WHBZ) 
        values(?,?,?,?,?,
                ?,?,?,?,?,
                ?,?,?,?,?,
                ?,?,to_date('$statDate','$dateFormat'),?)"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 18, ps, java.sql.Types.VARCHAR)
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

  def PWYW_DMS_KXZDH_DET(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val dateFormat = statType match {
      case "DAY"    => "yyyyMMdd"
      case "WEEK"   => "yyyyMMdd"
      case "MONTH"  => "yyyyMM"
      case "SEASON" => "yyyyMM"
      case "HY"     => "yyyyMM"
      case "YEAR"   => "yyyy"
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','$dateFormat')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (BRK_ID,ZXSJ,CONTENT,ZXNR,DWBM,
        DWMC,DWJB,FEEDER_ID,FEEDER_NAME,TERM_ID,
        TERM_NAME,BRK_NAME,SFNW,TJRQ,WHBZ) 
        values(?,?,?,?,?,
              ?,?,?,?,?,
              ?,?,?,to_date('$statDate','$dateFormat'),?)"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.VARCHAR)
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

  def PWYW_DMS_XLZDH_DET(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val dateFormat = statType match {
      case "DAY"    => "yyyyMMdd"
      case "WEEK"   => "yyyyMMdd"
      case "MONTH"  => "yyyyMM"
      case "SEASON" => "yyyyMM"
      case "HY"     => "yyyyMM"
      case "YEAR"   => "yyyy"
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','$dateFormat')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,FEEDER_ID,FEEDER_NAME,
        CLJZ,SFNW,WHBZ,SFFG,TJRQ) 
        values(?,?,?,?,?,
        ?,?,?,?,to_date('$statDate','$dateFormat'))"""
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
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.NUMERIC)
          
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

  def PWYW_DMS_XLGZ_DET(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val dateFormat = statType match {
      case "DAY"    => "yyyyMMdd"
      case "WEEK"   => "yyyyMMdd"
      case "MONTH"  => "yyyyMM"
      case "SEASON" => "yyyyMM"
      case "HY"     => "yyyyMM"
      case "YEAR"   => "yyyy"
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','$dateFormat')"""
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = s"""insert into pwyw.$tableName (GZSJ,CONTENT,DWBM,DWMC,DWJB,FEEDER_ID,FEEDER_NAME,TERM_ID,TERM_NAME,BRK_ID,BRK_NAME,SFNW,TJRQ,WHBZ) 
        values(?,?,?,?,?,
              ?,?,?,?,?,
              ?,?,to_date('$statDate','$dateFormat'),?)"""
      try {
        Class.forName(utils.cp.getProperty("driver"))
        conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
        ps = conn.prepareStatement(sql)
        conn.setAutoCommit(false)
        var iCnt: Int = 0
        row.foreach(data => {
          iCnt += 1
          OracleUtils.ExecPS(data, 1, ps, java.sql.Types.TIMESTAMP)
          OracleUtils.ExecPS(data, 2, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 3, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 4, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 5, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 6, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 7, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 8, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 9, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 10, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.VARCHAR)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.VARCHAR)
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

  def PWYW_DMS_YXZB(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val TJJSRQ = statType match {
      case "WEEK" =>
        val calendar = Calendar.getInstance
        val date = utils.sdfDay.parse(statDate)
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, 6)
        utils.sdfDay.format(calendar.getTime)
      case _ => null
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = statType match {
      case "DAY"  => s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','yyyyMMdd')"""
      case "WEEK" => s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDate','yyyyMMdd')"""
      case _      => s"""delete pwyw.$tableName where TJRQ = '$statDate' """
    }
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = statType match {
        case "DAY" => s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,ZD_ZS,ZD_ZXSC,ZD_YZXSC,ZD_LXDSZS,ZD_PJZXL,YKCZ_ZCS,YKCZ_CGCS,YKCZ_SBCS,YX_BWZS,
          YX_SOE_PPZS,KXZDH_QDZS,KXZDH_CGZXZS,YKCZ_CGL,YKCZ_CGL_DF,YX_ZQL,YX_ZQL_DF,KXZDH_CGL,KXZDH_CGL_DF,ZD_PJZXL_DF,PDZDH_YXZB_DF,CNW,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDate','yyyyMMdd'))"""
        case "WEEK" => s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,ZD_ZS,ZD_ZXSC,ZD_YZXSC,ZD_LXDSZS,ZD_PJZXL,YKCZ_ZCS,YKCZ_CGCS,YKCZ_SBCS,YX_BWZS,
          YX_SOE_PPZS,KXZDH_QDZS,KXZDH_CGZXZS,YKCZ_CGL,YKCZ_CGL_DF,YX_ZQL,YX_ZQL_DF,KXZDH_CGL,KXZDH_CGL_DF,ZD_PJZXL_DF,PDZDH_YXZB_DF,CNW,TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDate','yyyyMMdd'),to_date('$TJJSRQ','yyyyMMdd'))"""
        case _ => s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,ZD_ZS,ZD_ZXSC,ZD_YZXSC,ZD_LXDSZS,ZD_PJZXL,YKCZ_ZCS,YKCZ_CGCS,YKCZ_SBCS,YX_BWZS,
          YX_SOE_PPZS,KXZDH_QDZS,KXZDH_CGZXZS,YKCZ_CGL,YKCZ_CGL_DF,YX_ZQL,YX_ZQL_DF,KXZDH_CGL,KXZDH_CGL_DF,ZD_PJZXL_DF,PDZDH_YXZB_DF,CNW,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'$statDate')"""
      }
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

  def PWYW_DMS_JSGLZB(data: DataFrame, tableName: String, statDate: String, statType: String) {
    val TJJSRQ = statType match {
      case "WEEK" =>
        val calendar = Calendar.getInstance
        val date = utils.sdfDay.parse(statDate)
        calendar.setTime(date)
        calendar.add(Calendar.DAY_OF_YEAR, 6)
        utils.sdfDay.format(calendar.getTime)
      case _ => null
    }

    Class.forName(utils.cp.getProperty("driver"))
    val conn = DriverManager.getConnection(utils.url113, utils.cp.getProperty("user"), utils.cp.getProperty("password"))
    val delete = statType match {
      case "DAY"  => s"""delete pwyw.$tableName where TJRQ = to_date('$statDate','yyyyMMdd')"""
      case "WEEK" => s"""delete pwyw.$tableName where TJKSRQ = to_date('$statDate','yyyyMMdd')"""
      case _      => s"""delete pwyw.$tableName where TJRQ = '$statDate' """
    }
    val ps = conn.prepareStatement(delete)
    ps.execute
    ps.close
    conn.close

    data.foreachPartition(row => {
      var conn: Connection = null
      var ps: PreparedStatement = null
      val sql = statType match {
        case "DAY" => s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,XLZS,PDZDH_XLZS,PDZDH_FGS,KXZDH_GNPZ_XLZS,PDXL_GZZS,KXZDH_GZCL_QDZS,GZZDPD_CLL,
          GZZDPD_CLL_DF,PDZDH_FGL,PDZDH_FGL_DF,KXZDH_XLFGL,KXZDH_XLFGL_DF,JSGLZB_DF,CNW,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDate','yyyyMMdd'))"""
        case "WEEK" => s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,XLZS,PDZDH_XLZS,PDZDH_FGS,KXZDH_GNPZ_XLZS,PDXL_GZZS,KXZDH_GZCL_QDZS,GZZDPD_CLL,
          GZZDPD_CLL_DF,PDZDH_FGL,PDZDH_FGL_DF,KXZDH_XLFGL,KXZDH_XLFGL_DF,JSGLZB_DF,CNW,TJKSRQ,TJJSRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,to_date('$statDate','yyyyMMdd'),to_date('$TJJSRQ','yyyyMMdd'))"""
        case _ => s"""insert into pwyw.$tableName (DWBM,DWMC,DWJB,XLZS,PDZDH_XLZS,PDZDH_FGS,KXZDH_GNPZ_XLZS,PDXL_GZZS,KXZDH_GZCL_QDZS,GZZDPD_CLL,
          GZZDPD_CLL_DF,PDZDH_FGL,PDZDH_FGL_DF,KXZDH_XLFGL,KXZDH_XLFGL_DF,JSGLZB_DF,CNW,TJRQ) 
        values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'$statDate')"""
      }
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
          OracleUtils.ExecPS(data, 11, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 12, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 13, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 14, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 15, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 16, ps, java.sql.Types.NUMERIC)
          OracleUtils.ExecPS(data, 17, ps, java.sql.Types.NUMERIC)
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