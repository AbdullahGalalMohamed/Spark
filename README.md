# Apache Spark with scala project

In my project i loaded [Dataset](https://www.kaggle.com/gagazet/path-of-exile-league-statistic) into dataframes and did some analysis using a spark with scala language, i used IntelliJ IDEA in cloudera VM.

### Table of Contents 
---

- [Spark definition](#apache-spark)
- [Dataset description](#dataset)
- [Technologies](#technologies)
- [Quick Start](#install)

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
