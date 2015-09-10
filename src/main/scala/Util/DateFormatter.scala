package Util

/**
 * Created by damien on 9/9/2015.
 * Created as an object so it is serializable
 */

import java.text.SimpleDateFormat

object DateFormatter {
   def formatDate = (str:String) => {
     new java.sql.Date((new SimpleDateFormat("yyyy-MM-dd")).parse(str).getTime)
   }
}
