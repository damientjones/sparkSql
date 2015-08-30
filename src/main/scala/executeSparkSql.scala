/**
 * Created by damien on 8/19/2015.
 */

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class executeSparkSql {
  private val ed = new getEmpData
  private val dd = new getDeptData
  private val sc = createSparkContext.createContext

  def main = {
    createSparkContext.createHiveContext(sc)
    val hc = createSparkContext.getHiveContext
    val empDf=ed.getData(sc)
    val deptDf=dd.getData(sc)
    empDf.registerTempTable("emp")
    deptDf.registerTempTable("dept")
    val empDeptDf=hc.sql("SELECT e.firstName,e.lastName,e.deptId,e.salary,d.department FROM emp e JOIN dept d ON e.deptId = d.deptId").cache()
    println(empDeptDf.withColumn("firstSalary",first("salary").over(Window.partitionBy("department").orderBy("firstName"))).show)
    println(empDeptDf.select(empDeptDf("firstName"),empDeptDf("lastName"),empDeptDf("department")).show)
    println(empDeptDf.groupBy("department").count.show)
    println(empDeptDf.groupBy("department").sum("salary").show)
    println(empDeptDf.groupBy("department").agg(max("salary"),avg("salary")).show)
    println(empDeptDf.orderBy("department").show)
    empDeptDf.toJSON
  }
}
