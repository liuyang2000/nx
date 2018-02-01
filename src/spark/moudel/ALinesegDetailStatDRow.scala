package spark.moudel

case class ALinesegDetailStatDProcessRow(val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val POWEROFF_TIME: java.lang.Long,
                                         val POWERON_TIME: java.lang.Long, val POWER_OFF_TYPE: java.lang.Integer, val LOSS_POWER: java.lang.Float)

case class ALinesegDetailStatDRow(val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val POWEROFF_TIME: java.lang.Long,
                                  val POWERON_TIME: java.lang.Long, val POWER_OFF_TYPE: java.lang.Integer, val PLAN_OFF_TYPE: java.lang.Integer,
                                  val POWEROFF_TOTAL_TIME: java.lang.Float, val LOSS_POWER: java.lang.Float)

case class ALinesegTotalStatDProcessRow(val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val POWER_OFF_TYPE: java.lang.Integer,
                                        val PLAN_OFF_TYPE: java.lang.Integer, val POWEROFF_TOTAL_TIME: java.lang.Float, val LOSS_POWER: java.lang.Float,
                                        val POWEROFF_ALONE: String)

case class ALinesegTotalStatDRow(val LINE_SEG_NO: String, val PI_LINE_NO: String, val PI_ORG_NO: String, val POWEROFF_CNT: java.lang.Integer,
                                 val POWEROFF_ALONE_CONS_CNT: java.lang.Integer, val POWEROFF_NORMAL_CNT: java.lang.Integer, val PLAN_POWEROFF_CNT: java.lang.Integer,
                                 val PLAN_POWEROFF_DURATION: java.lang.Float,
                                 val PLAN_LOSS_POWER: java.lang.Float,
                                 val TEMP_POWEROFF_CNT: java.lang.Integer,
                                 val TEMP_POWEROFF_DURATION: java.lang.Float,
                                 val TEMP_LOSS_POWER: java.lang.Float,
                                 val PROD_POWEROFF_CNT: java.lang.Integer,
                                 val PROD_POWEROFF_DURATION: java.lang.Float,
                                 val PROD_LOSS_POWER: java.lang.Float)

case class ALinesegTotalStatDStatRateRow(val LINE_SEG_NO: String,
                                         val PI_ORG_NO: String,
                                         val PI_LINE_NO: String,
                                         val POWEROFF_CNT: java.lang.Integer,
                                         val POWEROFF_ALONE_CONS_CNT: java.lang.Integer,
                                         val POWEROFF_NORMAL_CNT: java.lang.Integer,
                                         val PLAN_POWEROFF_CNT: java.lang.Integer,
                                         val PLAN_POWEROFF_DURATION: java.lang.Float,
                                         val PLAN_LOSS_POWER: java.lang.Float,
                                         val TEMP_POWEROFF_CNT: java.lang.Integer,
                                         val TEMP_POWEROFF_DURATION: java.lang.Float,
                                         val TEMP_LOSS_POWER: java.lang.Float,
                                         val PROD_POWEROFF_CNT: java.lang.Integer,
                                         val PROD_POWEROFF_DURATION: java.lang.Float,
                                         val PROD_LOSS_POWER: java.lang.Float)
