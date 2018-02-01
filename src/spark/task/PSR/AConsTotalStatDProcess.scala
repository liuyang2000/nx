package spark.task.PSR

import java.util.Date
import scala.reflect.runtime.universe
import org.apache.spark.sql.functions._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.hive.HiveContext
import spark.moudel.ATgDetailStatDRow
import spark.moudel.ATgTotalStatDRow
import spark.util.utils
import org.apache.spark.sql.DataFrame
import spark.moudel.AConsDetailStatDRow
import spark.moudel.AConsTotalStatDRow

class AConsTotalStatDProcess(hc: HiveContext, dataReader: PSRDataReader) extends Serializable {
  import hc.implicits._
  
  def execute(powerOnffData: Dataset[AConsDetailStatDRow]) = {
    powerOnffData.groupBy($"CONS_NO").mapGroups((id, i) => {
      val seq = i.toSeq
      val POWEROFF_CNT = seq.length
      var PLAN_POWEROFF_CNT = 0
      var PLAN_POWEROFF_DURATION = 0f
      var PLAN_LOSS_POWER = 0f
      var TEMP_POWEROFF_CNT = 0
      var TEMP_POWEROFF_DURATION = 0f
      var TEMP_LOSS_POWER = 0f
      var PROD_POWEROFF_CNT = 0
      var PROD_POWEROFF_DURATION = 0f
      var PROD_LOSS_POWER = 0f
      for (r <- seq) {
        if (r.POWER_OFF_TYPE == 2) {
          PLAN_POWEROFF_CNT += 1
          PLAN_POWEROFF_DURATION += r.POWEROFF_TOTAL_TIME
          PLAN_LOSS_POWER += r.LOSS_POWER
        }
        if (r.POWER_OFF_TYPE == 1) {
          PROD_POWEROFF_CNT += 1
          PROD_POWEROFF_DURATION += r.POWEROFF_TOTAL_TIME
          PROD_LOSS_POWER += r.LOSS_POWER
        }
        if (r.POWER_OFF_TYPE == 3) {
          TEMP_POWEROFF_CNT += 1
          TEMP_POWEROFF_DURATION += r.POWEROFF_TOTAL_TIME
          TEMP_LOSS_POWER += r.LOSS_POWER
        }
      }
      //val CONS_NO: String, val CONS_NAME: String, val ELEC_ADDR: String, val PI_ORG_NO: String,  val POWEROFF_CNT: java.lang.Integer
      AConsTotalStatDRow(id.getString(0), POWEROFF_CNT,seq(0).PI_ORG_NO,
        PLAN_POWEROFF_CNT,
        PLAN_POWEROFF_DURATION,
        PLAN_LOSS_POWER,
        TEMP_POWEROFF_CNT,
        TEMP_POWEROFF_DURATION,
        TEMP_LOSS_POWER,
        PROD_POWEROFF_CNT,
        PROD_POWEROFF_DURATION,
        PROD_LOSS_POWER)
    })
  }

  def addID(data: DataFrame) = {
    val addStrUDF = udf((str: String, str1: String) => str + str1)
    data.withColumn("STAT_DATE", lit(dataReader.dateStr))
      .withColumn("DATA_ID", addStrUDF($"TG_ID", lit(dataReader.dateStr)))
  }
  
}