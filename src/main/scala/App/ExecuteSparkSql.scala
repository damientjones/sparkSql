package App

/**
 * Created by damien on 8/19/2015.
 */

import Data.{DeptData, EmpData}
import Util.CreateSparkContext
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class ExecuteSparkSql {
  private val sc = CreateSparkContext.createContext
  private val ed = new EmpData(sc)
  private val dd = new DeptData(sc)

  def main = {
    CreateSparkContext.createHiveContext(sc)
    val hc = CreateSparkContext.getHiveContext
    val empDf=ed.getData
    val deptDf=dd.getData
    empDf.registerTempTable("emp")
    deptDf.registerTempTable("dept")
    val empDeptDf=hc.sql("SELECT e.firstName,e.lastName,e.managerId,e.deptId,e.salary,e.hireDate,d.department FROM emp e JOIN dept d ON e.deptId = d.deptId").cache()
    println(empDeptDf.withColumn("firstSalary",first("salary").over(Window.partitionBy("department").orderBy("firstName"))).show)
    println(empDeptDf.select(empDeptDf("firstName"),empDeptDf("lastName"),empDeptDf("department")).show)
    println(empDeptDf.groupBy("department").count.show)
    println(empDeptDf.groupBy("department").sum("salary").show)
    println(empDeptDf.groupBy("department").agg(max("salary"),avg("salary")).show)
    println(empDeptDf.orderBy("department").show)
    empDeptDf.toJSON
  }
}
