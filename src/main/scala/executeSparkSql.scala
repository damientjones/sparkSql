/**
 * Created by damien on 8/19/2015.
 */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class executeSparkSql {
  def main = {
    val sc = createSparkContext.createContext
    createSparkContext.createHiveContext(sc)
    val empDf=getEmpData.getData(sc)
    val deptDf=getDeptData.getData(sc)
    val empDeptDf=empDf.join(deptDf,empDf("deptId")===deptDf("deptId")).cache
    println(empDeptDf.withColumn("firstSalary",first("salary").over(Window.partitionBy("department").orderBy("firstName"))).show)
    println(empDeptDf.select(empDeptDf("firstName"),empDeptDf("lastName"),empDeptDf("department")).show)
    println(empDeptDf.groupBy("department").count.show)
    println(empDeptDf.groupBy("department").sum("salary").show)
    println(empDeptDf.groupBy("department").agg(max("salary"),avg("salary")).show)
    println(empDeptDf.orderBy("department").show)
    empDeptDf.toJSON
  }
}
