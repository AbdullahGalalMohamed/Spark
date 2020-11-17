# Apache Spark with scala project

In my project I loaded [Dataset](https://www.kaggle.com/gagazet/path-of-exile-league-statistic) into dataframes and did some analysis using a spark with scala language, i used IntelliJ IDEA in cloudera VM.

### Table of Contents 
---

- [Spark definition](#apache-spark)
- [Dataset description](#dataset)
- [Technologies](#technologies)
- [Quick Start](#install)
- [Reading Dataset](#reading-dataset)
- [Analysis and Visualization](#analysis-and-visualization)

### Apache Spark
---

**Apache Spark** is an open source parallel processing framework for running large-scale data analytics applications across clustered computers. It can handle both batch and real-time analytics and data processing workloads.

### Dataset
---

* Contains :
  - Dataset i choosed is [path of exile](https://www.kaggle.com/gagazet/path-of-exile-league-statistic) dataset. That contains statistics for 59,000 players who played path of exile. One file with 12 data sections.
  
* Column name and description  : 

   | Column name |                Description                |
   |:-----------:|:-----------------------------------------:|
   |     Rank    | Ranking of the player                     |
   |     Dead    | If player dead or not                     |
   |    Online   | Player playing online or offline          |
   |     Name    | Name of the player                        |
   |    Level    | Level of the player                       |
   |    Class    | Character who player chooses              |
   |      Id     | Id distinct for every player character    |
   |  Experience | Points, player takes when end challenge   |
   |   Account   | Account name distinct for every player    |
   |  Challenges | Number of challenges which player entered |
   |    Twitch   | It shows who stream video games           |
   |    Ladder   | Description below                         | 

  - One league - Harbinger, but 4 different types of divisions: 
    - Harbinger , Hardcore Harbinger , SSF Harbinger , SSF Harbinger HC 
Each division has own ladder with leaders

### Technologies
---

Project is created with:

- Scala 2.11.12
- JDK 1.8.0
- sbt 1.3.13
- SQL

###  Install
---

Steps to install scala in cloudera VM 

- Open the terminal and type: ```sudo yum install java-1.8.0-openjdk```
- Then type: ```sudo yum install java-1.8.0-openjdk-devel```
- Download [Intellij](https://www.jetbrains.com/idea/download/other.html) 2018.3.6 for Linux (tar.gz)
- Extract the downloaded package and move it to any directory like /tmp and open /bin directory of the extracted folder then run ./idea.sh script.
- Click install Scala 

![Quick start](https://scontent.fcai19-1.fna.fbcdn.net/v/t1.15752-9/125830325_863772741033810_2200855161068218723_n.jpg?_nc_cat=111&ccb=2&_nc_sid=ae9488&_nc_eui2=AeFhqsTM6hiDQLmUZ7ZhOJrDI2b4LfcEzq4jZvgt9wTOrrwfxXMuSlKcyGi04j3hfqMMrynwNtdUFnWtU_bZtsoF&_nc_ohc=BeUl9_jLS04AX_iZejD&_nc_ht=scontent.fcai19-1.fna&oh=296d0a741742926d5441a0c4d69834da&oe=5FDA4216)
- Then start a new project
- Open **build.sbt** file and add the below lines to add Spark dependencies:
```
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
```
- Make spark object and put your code, then run

### Reading Dataset
---

We need to read data from csv file and load data to hive parquet table then read table into dataframe.

Steps :

- **Create Parquet table :** Go to Hue, impala Query UI, and execute the following querie :
```
CREATE TABLE hive_task_db.parquet_tb(
`rank` INT,
`dead` BOOLEAN,
`online` BOOLEAN,
`name` STRING,
`level` INT,
`class` STRING,
`id` STRING,
`experience` DOUBLE,
`account` STRING,
`challenges` INT,
`twitch` STRING,
`ladder` STRING)
STORED AS PARQUET;
```
- Add the following code to the Object file :
```
val spark = SparkSession
          .builder
          .appName("Hive task")
          .master("local[4]") /*Running spark locally with 4 cores */
          .config("spark.sql.warehouse.dir","thrift://quickstart.cloudera:9083") /*Connection to the running Hive server 
           metastore in Cloudera */
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
   
// read data from csv file
val people = spark.read.option("header", "true").option("nullValue","null").schema(customSchema)
.csv("/tmp/idea-IU-183.6156.11/poe_stats.csv")

// to solve overwrite mode issue
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

// load data to parquet table
people.write.mode("overwrite").saveAsTable("hive_task_db.parquet_tb")

// read table into dataframe
people = spark.read.table("hive_task_db.parquet_tb")
people.createOrReplaceTempView("people_tb")
```

### Analysis and Visualization
---

In this part i show some queries and output :

- Count number of player in distinct ladder and sort :
  - **Query :** ```spark.sql("select ladder, count(*) As count From people_tb group by ladder Order by count").show()```
  - **Output :**
  
    |       ladder       | count |
    |:------------------:|:-----:|
    | Hardcore Harbinger | 14905 |
    |      Harbinger     | 14918 |
    |  SSF Harbinger HC  | 14972 |
    |    SSF Harbinger   | 14981 |
  - **Chart :**
  
  ![Chart](https://scontent.fcai19-1.fna.fbcdn.net/v/t1.15752-9/125314195_394003615131915_1338388398384405498_n.jpg?_nc_cat=102&ccb=2&_nc_sid=ae9488&_nc_eui2=AeHh_SRa3Ox0imXnjME15-6_BpXbX6uKHKAGldtfq4ocoKVZVkwadK77wtMZv_QdQYnsgz6blOukjdxv7PSDUr7M&_nc_ohc=6PZRNjf7-HoAX_qEkzI&_nc_ht=scontent.fcai19-1.fna&oh=9370ab29f2a60d87d7a7a83d1172d515&oe=5FD8F60B)
  - **Output Description :** Shows the 4 divisions have approximately the same numbers of players

- Get classes name and count for the highest 5 in ladder = Harbinger and sort descending :
  - **Query :** ```spark.sql("select class , count(*) as count from people_tb where ladder == 'Harbinger' group by class order by count desc ").show(5)```
  - **Output :**
  
    |    class    | count |
    |:-----------:|:-----:|
    |  Pathfinder | 3428  |
    |  Berserker  | 2713  |
    |    Raider   | 1943  |
    |    Slayer   | 1706  |
    | Necromancer | 1207  |
  - **Chart :**
  
  ![Chart](https://scontent.fcai19-1.fna.fbcdn.net/v/t1.15752-9/125981781_283661319690482_2822489419850493508_n.jpg?_nc_cat=105&ccb=2&_nc_sid=ae9488&_nc_eui2=AeGWN2yGfjU1ly69wyl-DyBFpcTmivDzcKylxOaK8PNwrH3BCW7IsD446PUdDE5PuGPwrYglOZh4xogonm-ku7Lw&_nc_ohc=NvAHBEpLfssAX8yBMD4&_nc_ht=scontent.fcai19-1.fna&oh=c80fd47b9906346d1dfb7b84e474cd0a&oe=5FD9F4B6)
  - **Output Description :** Shows the Pathfinder is the highest in ladder = Harbinger

- Top 3 ranked players per division and are not twitch streamers :
  - **Query :** ```spark.sql("select * from (select name, rank, ladder, row_number() over (partition by ladder order by rank) as row_number from people_tb where twitch is null) ranks where row_number <= 3").show()```
  - **Output :**
  
    |         name         | rank |       ladder       | row_number |
    |:--------------------:|:----:|:------------------:|:----------:|
    |      ChiroxPrime     |   1  |    SSF Harbinger   |      1     |
    |     FutonBlewAway    |   3  |    SSF Harbinger   |      2     |
    |        kinther       |   4  |    SSF Harbinger   |      3     |
    |      ShiruHBSSF      |   3  |  SSF Harbinger HC  |      1     |
    |        Dev_XD        |   4  |  SSF Harbinger HC  |      2     |
    |    midget_SPINNER_   |   7  |  SSF Harbinger HC  |      3     |
    |     Grosseplotee     |  16  | Hardcore Harbinger |      1     |
    |       Rolllfer       |  24  | Hardcore Harbinger |      2     |
    | LauraPalmerRESURR... |  27  | Hardcore Harbinger |      3     |
    |  Cool_NecroIsFineNow |   2  |      Harbinger     |      1     |
    |   MISTER_EXECUTION_  |  11  |      Harbinger     |      2     |
    |    CaveReTienHarb    |  12  |      Harbinger     |      3     |

- Top 3 class in death count for max leveled characters for each division :
  - **Query :** ```spark.sql("select * from (select ladder, class, count , row_number() over (partition by ladder order by count DESC) as row_number from (select ladder, level, class, count(*) as count from people_tb where dead == true and level == 100 group by ladder, class, level))ranks where row_number <= 3").show()```
  - **Output :**
  
    |       ladder       |    class    | count | row_number |
    |:------------------:|:-----------:|:-----:|:----------:|
    |  SSF Harbinger HC  | Necromancer |   1   |      1     |
    |  SSF Harbinger HC  |    Raider   |   1   |      2     |
    | Hardcore Harbinger |   Guardian  |   5   |      1     |
    | Hardcore Harbinger |  Gladiator  |   4   |      2     |
    | Hardcore Harbinger | Necromancer |   3   |      3     |
  - **Output Description :**  Shows there are two divisions only have players dead in dataset and in “SSF Harbinger HC” there are two only players have level 100 and dead.
  
- Count number of finished challenges for each division :
  - **Query :** ```spark.sql("select ladder,sum(challenges) From people_tb group by ladder").show()```
  - **Output :**
  
    |       ladder       |   sum  |
    |:------------------:|:------:|
    |   SSF Harbinger    | 331780 |
    |  SSF Harbinger HC  | 303737 |
    | Hardcore Harbinger | 404949 |
    |      Harbinger     | 499338 |
   - **Chart :**
  
  ![Chart](https://scontent.fcai19-1.fna.fbcdn.net/v/t1.15752-9/125871087_288431232477249_1420064188091589239_n.jpg?_nc_cat=111&ccb=2&_nc_sid=ae9488&_nc_eui2=AeHQGJ_V4cA1Bm6GGo9hOY3SXQDkzGZk9RhdAOTMZmT1GBnR_c-lf1v5mFb0s4asv0PRXS9CZmiimBfbOvhwqfR3&_nc_ohc=FShESJ6x9LIAX8lCopG&_nc_ht=scontent.fcai19-1.fna&oh=9f42a12003343b253e88182aefb43bc2&oe=5FDABF9C)
  - **Output Description :**  Shows Harbinger division have the large number of players finished challenges.
