package spark.task.PSR

import scala.reflect.runtime.universe
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import spark.moudel.ALinesegDetailStatDRow
import spark.moudel.ALinesegTotalStatDRow

class ALinesegTotalStatDProcess(hc: HiveContext, dataReader: PSRDataReader) extends Serializable {
  import hc.implicits._

  def execute(aTgTotalData: Dataset[ALinesegDetailStatDRow]) = {
    aTgTotalData.groupBy($"LINE_SEG_NO", $"PI_ORG_NO").mapGroups((id, i) => {
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
      ALinesegTotalStatDRow(id.getString(0), seq(0).PI_LINE_NO, id.getString(1), POWEROFF_CNT, null, null, PLAN_POWEROFF_CNT,
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

  //  def statRate(data: DataFrame, tgData: DataFrame) = {
  //    val addStrUDF = udf((str: String, str1: String, str2: String) => str + str1 + str2)
  //    val poweroff_cis_cons_cnt = tgData.select("LINE_SEG_NO", "PI_ORG_NO", "TG_ID").distinct().groupBy("LINE_SEG_NO", "PI_ORG_NO").count.withColumnRenamed("count", "POWEROFF_CIS_CONS_CNT").drop("TG_ID")
  //    data.join(poweroff_cis_cons_cnt, Seq("LINE_SEG_NO", "PI_ORG_NO"), "left_outer").withColumn("POWEROFF_PI_CONS_CNT", $"POWEROFF_CIS_CONS_CNT")
  //      .join(dataReader.PIS_CONS_CNT_LINESEG, Seq("LINE_SEG_NO", "PI_ORG_NO")).join(dataReader.CIS_CONS_CNT_LINESEG, Seq("LINE_SEG_NO", "PI_ORG_NO")).withColumn("INTEGRITY_RATE", $"POWEROFF_PI_CONS_CNT" / $"PI_CONS_CNT")
  //      .withColumn("INTEGRITY_ALONE_CONS_RATE", $"POWEROFF_ALONE_CONS_CNT" / $"PI_CONS_CNT").withColumn("INTEGRITY_NORMAL_RATE", $"POWEROFF_NORMAL_CNT" / $"PI_CONS_CNT")
  //      .withColumn("DATA_ID", addStrUDF($"LINE_SEG_NO", $"PI_ORG_NO", lit(dataReader.dateStr)))
  //  }
}