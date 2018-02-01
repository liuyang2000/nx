package spark.util

trait NullableStatProcess extends Serializable {
  protected def add(a1: java.lang.Integer, a2: java.lang.Integer): java.lang.Integer = {
    if (a1 == null)
      if (a2 == null) null
      else a2
    else if (a2 == null) a1
    else a1 + a2
  }

  protected def add(a1: java.lang.Float, a2: java.lang.Float): java.lang.Float = {
    if (a1 == null)
      if (a2 == null) null
      else a2
    else if (a2 == null) a1
    else a1 + a2
  }

  protected def add(a1: java.lang.Long, a2: java.lang.Long): java.lang.Long = {
    if (a1 == null)
      if (a2 == null) null
      else a2
    else if (a2 == null) a1
    else a1 + a2
  }

  protected def add(a1: java.lang.Double, a2: java.lang.Double): java.lang.Double = {
    if (a1 == null)
      if (a2 == null) null
      else a2
    else if (a2 == null) a1
    else a1 + a2
  }

  protected def minus(a1: java.lang.Float, a2: java.lang.Float, tFactor: java.lang.Float): java.lang.Float = {
    if (a1 == null || a2 == null) null
    else (a1 - a2) * tFactor
  }

  //  def mul(a1: java.lang.Float, a2: java.lang.Float): java.lang.Float = {
  //      if(a1 == null || a2 == null) null
  //      else a1 * a2
  //    }

  protected def calcMax(e1: java.lang.Float, e2: java.lang.Float, time1: String, time2: String) = {
    if (e1 == null)
      if (e2 == null) (null, null)
      else (e2, time2)
    else if (e2 == null) (e1, time1)
    else if (e1 > e2) (e1, time1)
    else (e2, time2)
  }

  protected def calcMax(e1: java.lang.Float, e2: java.lang.Float) = {
    if (e1 == null)
      if (e2 == null) null
      else e2
    else if (e2 == null) e1
    else if (e1 > e2) e1
    else e2
  }

  protected def calcMin(e1: java.lang.Float, e2: java.lang.Float, time1: String, time2: String) = {
    if (e1 == null)
      if (e2 == null) (null, null)
      else (e2, time2)
    else if (e2 == null) (e1, time1)
    else if (e1 < e2) (e1, time1)
    else (e2, time2)
  }

  protected def calcMin(e1: java.lang.Float, e2: java.lang.Float) = {
    if (e1 == null)
      if (e2 == null) null
      else e2
    else if (e2 == null) e1
    else if (e1 < e2) e1
    else e2
  }

  protected def calcAvg(a1: java.lang.Float, a2: java.lang.Float): java.lang.Float = {
    if (a1 == null)
      if (a2 == null) null
      else a2
    else if (a2 == null) a1
    else (a1 + a2) / 2
  }

}