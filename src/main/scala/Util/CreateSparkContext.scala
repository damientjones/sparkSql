package Util

/**
 * Created by damien on 8/19/2015.
 * Created as object so that only one version exists and
 * all functions can call it without needing to instantiate and pass
 * around an instantiated object
 * Change logging level to errors only
 * Create a spark context using 4 threads with Spark App as the name
 */

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object CreateSparkContext {
  val level = Level.ERROR
  Logger.getLogger("org").setLevel(level)
  Logger.getLogger("akka").setLevel(level)
  /** Need to share one Hive context across all operations otherwise
  * Spark will throw failures because only one context can connect
  * to Derby */
  private var hc : HiveContext = _
  private var sc : SparkContext = _

  def createContext  {
    if (sc==null) {
      val appName: String = "Spark App"
      val master: String = "local[4]"
      /* returns spark context object */
      sc = new SparkContext(new SparkConf().setAppName(appName).setMaster(master))
      hc = new HiveContext(sc)
    }
  }

  def getSparkContext : SparkContext = {
    sc
  }

  def getHiveContext : HiveContext = {
      hc
  }
}