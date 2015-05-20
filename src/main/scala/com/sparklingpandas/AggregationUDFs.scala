/*
 * These are additional aggregation UDFs we want to make available that are not available in standard Spark SQL
 */
package com.sparklingpandas;

import scala.collection.JavaConversions._

import scala.math._

import org.apache.commons.math.stat.StatUtils
import org.apache.commons.math.stat.descriptive.moment.{ Kurtosis => ApacheKurtosis}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.trees
import org.apache.spark.sql.catalyst.errors.TreeNodeException

case class Kurtosis(child: Expression) extends AggregateExpression with trees.UnaryNode[Expression] {
  def this() = this(null)

  override def nullable: Boolean = true
  override def dataType: DataType = DoubleType
  override def toString: String = s"Kurtosis($child)"
  override def newInstance() =  new KurtosisFunction(child, this)
}

case class KurtosisFunction(child: Expression, base: AggregateExpression) extends AggregateFunction {
  var data = scala.collection.mutable.ArrayBuffer.empty[Any]
  override def update(input: Row): Unit = {
    data += child.eval(input)
  }

  private def toDouble(x: Any): Double = {
    /* A helper function to get a Spark SQL Numeric type into a Double
     * ideally we would call numeric but its protected, lets round trip it through strings :(
     * TODO: Do this without roundtriping through json :(
     */
    x.asInstanceOf[DataType].json.toDouble
  }
  override def eval(input: Row): Any = {
    if (data.isEmpty) {
      null
    } else {
      val inputAsDoubles = data.toList.map(toDouble).toArray
      val apacheKurtosis = new ApacheKurtosis()
      val result = apacheKurtosis.evaluate(inputAsDoubles, 0, inputAsDoubles.size)
      Cast(Literal(result), DataTypes.DoubleType).eval(null)
    }
  }
}
