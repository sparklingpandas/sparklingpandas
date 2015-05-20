package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.NumericType

// This will break, because we do evil things. but we do them in the name of fun
object EvilSqlTools {
  def getExpr(c: Column): Expression = c.expr
  def toDouble(e: NumericType): Double = e.numeric.asInstanceOf[Numeric[Any]].toDouble()
}
