package spark.moudel

import java.lang.Float

case class ATgDetailStatDProcessRow(val TG_ID: String, val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val TG_CAP: java.lang.Integer, val PLAN_POWERONOFF_FLAG: String,
                                    val PROD_POWERONOFF_FLAG: String, val POWEROFF_TIME: String,
                                    val POWERON_TIME: String)

case class ATgDetailStatDProcessRow1(val TG_ID: String, val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val TG_CAP: java.lang.Integer, val POWEROFF_TIME: java.lang.Long,
                                     val POWERON_TIME: java.lang.Long, val POWER_OFF_TYPE: java.lang.Integer)

case class ATgDetailStatDRow(val TG_ID: String, val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val TG_CAP: java.lang.Float, val POWER_OFF_TYPE: java.lang.Integer, val PLAN_OFF_TYPE: java.lang.Integer,
                             val POWEROFF_TOTAL_TIME: java.lang.Float, val LOSS_POWER: java.lang.Float, val POWEROFF_TIME: java.lang.Long,
                             val POWERON_TIME: java.lang.Long)

case class ATgTotalStatDRow(val TG_ID: String, val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val TG_CAP: java.lang.Float, val POWEROFF_CNT: java.lang.Integer,
                            val PLAN_POWEROFF_CNT: java.lang.Integer,
                            val PLAN_POWEROFF_DURATION: java.lang.Float,
                            val PLAN_LOSS_POWER: java.lang.Float,
                            val TEMP_POWEROFF_CNT: java.lang.Integer,
                            val TEMP_POWEROFF_DURATION: java.lang.Float,
                            val TEMP_LOSS_POWER: java.lang.Float,
                            val PROD_POWEROFF_CNT: java.lang.Integer,
                            val PROD_POWEROFF_DURATION: java.lang.Float,
                            val PROD_LOSS_POWER: java.lang.Float)

case class ATgTotalStatRow(val TG_ID: String, val LINE_SEG_NO: String, val PI_ORG_NO: String, val TG_CAP: java.lang.Float, val POWEROFF_CNT: java.lang.Integer,
                           val PLAN_POWEROFF_CNT: java.lang.Integer,
                           val PLAN_POWEROFF_DURATION: java.lang.Float,
                           val PLAN_LOSS_POWER: java.lang.Float,
                           val TEMP_POWEROFF_CNT: java.lang.Integer,
                           val TEMP_POWEROFF_DURATION: java.lang.Float,
                           val TEMP_LOSS_POWER: java.lang.Float,
                           val PROD_POWEROFF_CNT: java.lang.Integer,
                           val PROD_POWEROFF_DURATION: java.lang.Float,
                           val PROD_LOSS_POWER: java.lang.Float)