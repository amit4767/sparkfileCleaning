package com.sparkTutorial.rdd


import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types._

/** Find the movies with the most ratings. */
object EmployeecleansingDataSets {
  

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("EmployeeSalary")
      .master("local[3]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val lines = spark.sparkContext.textFile("emp.txt")

    val headerColumns = lines.first().split(",").to[List]

    def dfSchema(columnNames: List[String]): StructType = {
      StructType(
        Seq(
          StructField(name = "name", dataType = StringType, nullable = false),
          StructField(name = "age", dataType = IntegerType, nullable = false),
          StructField(name = "salary", dataType = DoubleType, nullable = false),
          StructField(name = "benefits", dataType = IntegerType, nullable = false),
          StructField(name = "department", dataType = StringType, nullable = false)

        )
      )
    }

    final case class Employee(name:String,age:Int,salary:Double,benefits:Int,department:String)

    def row(line: List[String]): Row = {

      Row(line(0).replaceAll("\"","").trim,
        line(1).replaceAll("\"","").trim.toInt,
        line(2).replaceAll("\"","").trim.toDouble,
        line(3).replaceAll("\"","").trim.toInt,
        line(4).replaceAll("\"","").trim)

    }

    def filterRow(line: List[String])  = {
      if(line.size == 5) {
        try {
          Row(line(0).trim, line(1).replaceAll("\"","").trim.toInt,
            line(2).replaceAll("\"","").trim.toDouble,
            line(3).replaceAll("\"","").trim.toInt, line(4).trim)
          true
        }catch {
          case e: Exception => e.printStackTrace() ;false
        }

      }else {
        false
      }
    }

    val schema = dfSchema(headerColumns)
    //Cast all the columns to its type from the list

    val data2 = lines
        .mapPartitionsWithIndex((index, element) => if (index == 0 ) element.drop(1) else element) // skip header
        .map(_.split(",").to[List]).filter(filterRow)



      val data  = data2.map(row)


    val dataFrame = spark.createDataFrame(data, schema)

    dataFrame.printSchema()

    dataFrame.show()
    // Stop the session
    spark.stop()
  }
  
}

