/*
 * These are additional aggregation UDFs we want to make available that are not available in standard Spark SQL
 */
package com.sparklingpandas;

import scala.collection.JavaConversions._

import scala.math._

import org.apache.commons.math3.stat.StatUtils
import org.apache.commons.math3.stat.descriptive.moment.{ Kurtosis => ApacheKurtosis}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.EvilSqlTools
import org.apache.spark.sql.types.{DoubleType, NumericType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException

// functions we want to be callable from python
object functions {
  def kurtosis(e: String): Column = new Column(Kurtosis(
    EvilSqlTools.getExpr(new Column(e))))
  def kurtosis(e: Column): Column = new Column(Kurtosis(EvilSqlTools.getExpr(e)))
  def registerUdfs(sqlCtx: SQLContext): Unit = {
    sqlCtx.udf.register("rowKurtosis", helpers.rowKurtosis _)
  }
}

// helper functions, not meant to be called directly from Python
object helpers {
  def rowKurtosis(x: Any*): Double = {
    val inputAsDoubles = x.map(Utils.toDouble)
    val inputArray = inputAsDoubles.toArray
    val apacheKurtosis = new ApacheKurtosis()
    apacheKurtosis.evaluate(inputArray, 0, inputArray.size)
  }
}

// Compute the kurtosis on columns
case class Kurtosis(child: Expression) extends AggregateExpression {
  def this() = this(null)

  override def children = child :: Nil
  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def toString: String = s"Kurtosis($child)"
  override def newInstance() =  new KurtosisFunction(child, this)
}

case class KurtosisFunction(child: Expression, base: AggregateExpression) extends AggregateFunction {
  def this() = this(null, null)

  var data = scala.collection.mutable.ArrayBuffer.empty[Any]
  override def update(input: Row): Unit = {
    data += child.eval(input)
  }

  override def eval(input: Row): Any = {
    if (data.isEmpty) {
      null
    } else {
      val inputAsDoubles = data.toList.map(Utils.toDouble)
      val inputArray = inputAsDoubles.toArray
      val apacheKurtosis = new ApacheKurtosis()
      val result = apacheKurtosis.evaluate(inputArray, 0, inputArray.size)
      Cast(Literal(result), DoubleType).eval(null)
    }
  }
}
