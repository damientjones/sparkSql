/**
 * Created by damien on 8/19/2015.
 */

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

object getDeptData {
  private val deptData = "department.csv"
  private val schema = StructType(
     StructField("deptId",IntegerType,false) ::
     StructField("department",StringType,false) ::Nil
  )
  def getData (sc : SparkContext) : DataFrame = {
    val deptRdd = sc.textFile(deptData).map(_.split(",")).map(x=>Row(x(0).toInt,x(1)))
    createSparkContext.getHiveContext.createDataFrame(deptRdd,schema)
  }
}
