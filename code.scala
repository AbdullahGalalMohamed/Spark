import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object task2 {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.INFO)

    val spark = SparkSession
	     .builder
	     .appName("Hive task")
	     .master("local[4]")
	     .config("spark.sql.warehouse.dir","thrift://quickstart.cloudera:9083")
	     .enableHiveSupport()
	     .getOrCreate()
	val customSchema = StructType(Array(
	     StructField("rank", IntegerType, true),
	     StructField("dead", BooleanType, true),
	     StructField("online", BooleanType, true),
	     StructField("name", StringType, true),
	     StructField("level", IntegerType, true),
	     StructField("class", StringType, true),
	     StructField("id", StringType, true),
	     StructField("experience", DoubleType, true),
	     StructField("account", StringType, true),
	     StructField("challenges", IntegerType, true),
	     StructField("twitch", StringType, true),
	     StructField("ladder", StringType, true)))
	
	val people = spark.read.option("header", "true").option("nullValue", "null").schema(customSchema).csv("/tmp/idea-IU-183.6156.11/poe_stats.csv")    
	
	// to solve overwrite mode issue
	spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")    
	
	people.write.mode("overwrite").saveAsTable("hive_task_db.parquet_tb")
	
	people = spark.read.table("hive_task_db.parquet_tb")
	people.createOrReplaceTempView("people_tb")

    
    // 1
    println("Select the name and challenges columns (challengs > 35)")
    spark.sql("select name, challenges From people_tb where challenges > 35").show()

    // 2
    println("count number of player in distinct ladder and sort")
    spark.sql("select ladder, count(*) As count From people_tb group by ladder Order by count").show()

    // 3
    println("Get classes name and count for the highest 5 in ladder = Harbinger and sort descending")
    spark.sql("select class , count(*) as count from people_tb where ladder == 'Harbinger' group by class order by count desc ").show(5)

    // 4
    println("count number of players dead")
    spark.sql("select dead , count(*) as count from people_tb group by dead").show()

    // 5
    println("count number of finished challenges for each division :")
    spark.sql("select ladder,sum(challenges) From people_tb group by ladder").show()

    // 6
    println("Show dependency between level and class of died characters. Only for SSF Harbinger HC divisions :")
    spark.sql("select level, class ,count(*) as count from people_tb where ladder == 'SSF Harbinger HC' and dead == true group by level, class order by count desc").show()

    // 7
    println("Select the top 10 players they have the largest experience, twitch != null and ladder = SSF Harbinger :")
    spark.sql("select name, twitch, experience From people_tb where ladder = 'SSF Harbinger' and twitch is not null Order by experience Desc ").show(10)

    // 8
    println("Number of players with at least 1 character in 2 or more divisions")
    spark.sql("select * from (select DISTINCT account, Count(count) over (partition by account) as n_character from (select account, ladder, count(*) as count from people_tb group by account, ladder)) where n_character > 1").show()

    // 9
    println("Top 3 ranked players per class per level")
    spark.sql("select * from (select name, rank, class, level, row_number() over (partition by class, level order by rank) as row_number from people_tb) ranks where row_number <= 3").show()

    // 10
    println("Top 3 ranked players per division and are not twitch streamers")
    spark.sql("select * from (select name, rank, ladder, row_number() over (partition by ladder order by rank) as row_number from people_tb where twitch is null) ranks where row_number <= 3").show()

    // 11
    println("Top 3 class in death count for max leveled characters for each division")
    spark.sql("select * from (select ladder, class, count , row_number() over (partition by ladder order by count DESC) as row_number from (select ladder, level, class, count(*) as count from people_tb where dead == true and level == 100 group by ladder, class, level))ranks where row_number <= 3").show()

    spark.stop()


  }
}
