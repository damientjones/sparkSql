/**
 * Created by damien on 8/19/2015.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

object getEmpData {
  private val empData = "employee.csv"
  private val schema = StructType(
    StructField("firstName",StringType,false) ::
    StructField("lastName",StringType,false) ::
    StructField("deptId",IntegerType,false) ::
    StructField("salary",IntegerType,false) :: Nil
  )
  def getData (sc : SparkContext) : DataFrame = {
    val empRdd = sc.textFile(empData).map(_.split(",")).map(x=>Row(x(0),x(1),x(2).toInt,x(3).toInt))
    createSparkContext.getHiveContext.createDataFrame(empRdd,schema)
  }
}
