package spark.moudel

import java.lang.Float


case class AConsDetailStatDRow(val CONS_NO: String, val CONS_NAME: String, val ELEC_ADDR: String, val PI_ORG_NO: String,
                              val POWER_OFF_TYPE: java.lang.Integer, val PLAN_OFF_TYPE: java.lang.Integer,val POWEROFF_TOTAL_TIME: java.lang.Float, 
                              val LOSS_POWER: java.lang.Float, val POWEROFF_TIME: java.lang.String,val POWERON_TIME: java.lang.String)

                              
case class AConsTotalStatDRow(val CONS_NO: String,  val POWEROFF_CNT: java.lang.Integer, val PI_ORG_NO : java.lang.String,
                            val PLAN_POWEROFF_CNT: java.lang.Integer,
                            val PLAN_POWEROFF_DURATION: java.lang.Float,
                            val PLAN_LOSS_POWER: java.lang.Float,
                            val TEMP_POWEROFF_CNT: java.lang.Integer,
                            val TEMP_POWEROFF_DURATION: java.lang.Float,
                            val TEMP_LOSS_POWER: java.lang.Float,
                            val PROD_POWEROFF_CNT: java.lang.Integer,
                            val PROD_POWEROFF_DURATION: java.lang.Float,
                            val PROD_LOSS_POWER: java.lang.Float)