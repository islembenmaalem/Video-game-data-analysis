// Databricks notebook source
// MAGIC %md
// MAGIC # <h1 style="font-family: Trebuchet MS; padding: 5px; font-size: 48px; color: gold; text-align: center; line-height: 1;"><b>Video Game Sales<span style="color: #000000"> Data Analysis </span></b><br><span style="color: gold; font-size: 24px"> (Islem Ben Maalem) </span></h1>
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC <center>
// MAGIC
// MAGIC ![Image Alt Text](https://tm.ibxk.com.br/2019/10/24/24222425488242.jpg?ims=704x264)
// MAGIC
// MAGIC </center>

// COMMAND ----------

// MAGIC %md
// MAGIC # **Introduction**
// MAGIC In this notebook we're going to analyst  Video Game Sales Data by doing so we're going to get some insights.
// MAGIC
// MAGIC I'm planning to go through feature (question) by feature and take a closer look at those features to inspect their relationships with previous features.

// COMMAND ----------

// MAGIC %md
// MAGIC ## **Table of Contents:**
// MAGIC
// MAGIC 1. [Context]()
// MAGIC 2. [Import Libraries]()
// MAGIC 3. [Read in Data Frame]()
// MAGIC 4. [Data Preprocessing]()
// MAGIC     2. [How many missing data points do we have?]()
// MAGIC     3. [Clean up the Data!]()
// MAGIC     4. [Convert columns]()
// MAGIC     5. [Recap Data]()
// MAGIC 5. [Data Analysis]()
// MAGIC     2. [Task:]()
// MAGIC         1. [Q: Découvrez combien de Platform, combien de Genre, combien de Publisher et combien de game sont dans les données.]()
// MAGIC         2. [Q: What was the best Year for sales? How much was earned that Year?]()
// MAGIC         3. [Q: Quels sont les genres de jeux vidéo les plus vendus dans le monde après 2005, et quelles sont les années et les chiffres de ventes correspondants?]()
// MAGIC         4. [Q: What is the most sold Genre?]()
// MAGIC         5. [Q: What video games are most often sold ?]()
// MAGIC         6. [Q: What Platform had the highest number of Global Sales?]()
// MAGIC         7. [Q: What are the top 10 Publisher?]()
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC # **Context**
// MAGIC
// MAGIC video game sales analytics is the practice of generating insights from sales data, trends, and metrics to set targets and forecast future sales performance. 
// MAGIC ## **Content**
// MAGIC ``Rank`` - Ranking of overall sales
// MAGIC
// MAGIC ``Name`` - The games name
// MAGIC
// MAGIC ``Platform`` - Platform of the games release (i.e. PC,PS4, etc.)
// MAGIC
// MAGIC ``Year`` - Year of the game's release
// MAGIC
// MAGIC ``Genre`` - Genre of the game
// MAGIC
// MAGIC ``Publisher`` - Publisher of the game
// MAGIC
// MAGIC ``NA_Sales`` - Sales in North America (in millions)
// MAGIC
// MAGIC ``EU_Sales`` - Sales in Europe (in millions)
// MAGIC
// MAGIC ``JP_Sales`` - Sales in Japan (in millions)
// MAGIC
// MAGIC ``Other_Sales`` - Sales in the rest of the world (in millions)
// MAGIC
// MAGIC ``Global_Sales`` - Total worldwide sales.
// MAGIC ## **Task:**
// MAGIC * Q: Découvrez combien de Platform, combien de Genre, combien de Publisher et combien de game sont dans les données.
// MAGIC * Q: What was the best Year for sales? How much was earned that Year?
// MAGIC * Q: Quels sont les genres de jeux vidéo les plus vendus dans le monde après 2005, et quelles sont les années et les chiffres de ventes correspondants?
// MAGIC * Q: What is the most sold Genre?
// MAGIC * Q: What video games are most often sold ?
// MAGIC * Q: What Platform had the highest number of Global Sales?
// MAGIC * Q: What are the top 10 Publisher?

// COMMAND ----------

// MAGIC %md
// MAGIC #
// MAGIC # **Import Necessary Libraries**

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # **Read in Data Frame**
// MAGIC
// MAGIC Let's see the data and how it looks.

// COMMAND ----------

val data = spark.read
  .option("header", "true")
  .csv("/FileStore/tables/vgsales.csv")


// COMMAND ----------

data.printSchema()

// COMMAND ----------

data.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC it looks like there's some missing values.

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # **Data Preprocessing**

// COMMAND ----------

val numRows = data.count()
val numCols = data.columns.length

println(s"Number of rows: $numRows")
println(s"Number of columns: $numCols")


// COMMAND ----------

// MAGIC %md
// MAGIC ### **How many missing data points do we have?**

// COMMAND ----------

data.columns.foreach { colName =>
  val countMissing = data.filter(col(colName).isNull || col(colName) === "N/A").count()
  println(s"Number of missing values in $colName: $countMissing")
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### **Clean up the Data!**

// COMMAND ----------

// MAGIC %md
// MAGIC #### since this missing value is not too big, let's just drop the missing value, I think this will not affect the data since the data is big enough. 

// COMMAND ----------

import org.apache.spark.sql.functions.col

var cleanedData = data
  .filter(col("Year").isNotNull && col("Year") =!= "N/A" && col("Publisher").isNotNull && col("Publisher") =!= "N/A")

cleanedData.columns.foreach { colName =>
  val countMissing = cleanedData.filter(col(colName).isNull || col(colName) === "N/A").count()
  println(s"Number of missing values in $colName: $countMissing")
}


// COMMAND ----------

// MAGIC %md
// MAGIC ### **Convert columns**

// COMMAND ----------

cleanedData = data
  .withColumn("Rank", col("Rank").cast(IntegerType))
  .withColumn("Year", col("Year").cast(IntegerType))
  .withColumn("NA_Sales", col("NA_Sales").cast(DoubleType))
  .withColumn("EU_Sales", col("EU_Sales").cast(DoubleType))
  .withColumn("JP_Sales", col("JP_Sales").cast(DoubleType))
  .withColumn("Other_Sales", col("Other_Sales").cast(DoubleType))
  .withColumn("Global_Sales", col("Global_Sales").cast(DoubleType))

// COMMAND ----------

cleanedData.printSchema()

// COMMAND ----------

println("Number of rows: ",cleanedData.count())
println("Number of columns: ",cleanedData.columns.length)


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ### **Recap Data**
// MAGIC * We have total  16598   and  11  columns

// COMMAND ----------

// MAGIC %md
// MAGIC Next, we will try to do some exploration. 

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # **Data Analysis**
// MAGIC ---

// COMMAND ----------

// MAGIC %md
// MAGIC # 
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;"> Découvrez combien de Platform, combien de Genre, combien de Publisher et combien de game sont dans les données.
// MAGIC
// MAGIC </p>
// MAGIC Let's have a look at categorical variables. How many unique values of these variables.

// COMMAND ----------

// Nombre de Platform
val nombrePlatform = cleanedData.select(countDistinct("Platform")).first().getLong(0)

// Nombre de Genre
val nombreGenre = cleanedData.select(countDistinct("Genre")).first().getLong(0)

// Nombre de Publisher
val nombrePublisher = cleanedData.select(countDistinct("Publisher")).first().getLong(0)

// Nombre de Name
val nombreName = cleanedData.select(countDistinct("Name")).first().getLong(0)

println(s"Nombre de Platform : $nombrePlatform")
println(s"Nombre de produits : $nombreGenre")
println(s"Nombre de Publisher : $nombrePublisher")
println(s"Nombre de Name : $nombreName")

// COMMAND ----------

// MAGIC
// MAGIC %md
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;">What was the best Year for sales? How much was earned that Year?
// MAGIC
// MAGIC </p>
// MAGIC

// COMMAND ----------

val bestYearSales = cleanedData
  .groupBy("Year")
  .sum("Global_Sales")
  .orderBy(desc("sum(Global_Sales)"))
  .first()

  

// COMMAND ----------

val worstYearSales = cleanedData
  .groupBy("Year")
  .sum("Global_Sales")
  .orderBy("sum(Global_Sales)")
  .first()

// COMMAND ----------

// MAGIC %md
// MAGIC  ***Answer:***
// MAGIC >   When viewed from the data above, $2008$  was the best year that had the highest number of sales,  compared to  $2017$  which was the worst year, but this is due to the lack of data in  $2017$  which caused a data imbalance.

// COMMAND ----------

// MAGIC
// MAGIC %md
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;">Quels sont les genres de jeux vidéo les plus vendus dans le monde après 2005, et quelles sont les années et les chiffres de ventes correspondants?
// MAGIC
// MAGIC </p>

// COMMAND ----------

val df1 = data.select("Genre", "Year", "NA_Sales", "EU_Sales", "JP_Sales", "Other_Sales")
val df2 = df1.filter(col("Year") > 2005)

val df3 = df2
  .selectExpr("Genre", "Year", "stack(4, 'NA_Sales', NA_Sales, 'EU_Sales', EU_Sales, 'JP_Sales', JP_Sales, 'Other_Sales', Other_Sales) as (sale, sales_value)")
  .orderBy(col("sales_value").desc)
  .groupBy("sale", "Genre")
  .agg(expr("first(Year)").alias("Year"), expr("first(sales_value)").alias("sales_value"))

df3.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;">What is the most sold Genre?
// MAGIC </p>

// COMMAND ----------

val mostSoldGenre = cleanedData
  .groupBy("Genre")
  .sum("Global_Sales")
  .orderBy(desc("sum(Global_Sales)"))
  .first()
  

// COMMAND ----------

// MAGIC %md
// MAGIC  ***Answer:***
// MAGIC > The genre that has the most sales in the above visualization is ``Action``, with total sales reaching 1751.9  .

// COMMAND ----------

// distribution du genre action pour les differents region
val actionSalesByRegion = cleanedData
  .filter(col("Genre") === "Action")
  .groupBy("Global_Sales", "Year")
  .sum("NA_Sales", "EU_Sales", "JP_Sales", "Other_Sales","Global_Sales")
  .orderBy( desc("Global_Sales"))

actionSalesByRegion.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;">What video games are most often sold ?
// MAGIC </p>

// COMMAND ----------

val mostSoldGames = cleanedData
  .groupBy("Name")
  .sum("Global_Sales")
  .orderBy(desc("sum(Global_Sales)"))
  .limit(5) 

mostSoldGames.show()

// COMMAND ----------

// MAGIC %md
// MAGIC * we will take every region by itself then globally

// COMMAND ----------

// MAGIC %md
// MAGIC  ***Answer:***
// MAGIC > games that are often sold simultaneously are Wii Sport and Grand Theft Auto V.

// COMMAND ----------

// MAGIC %md
// MAGIC ---
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;">What Platform had the highest number of Global Sales?
// MAGIC </p>

// COMMAND ----------

val platformWithHighestSales = cleanedData
  .groupBy("Platform")
  .sum("Global_Sales")
  .orderBy(desc("sum(Global_Sales)"))
  .limit(5)

platformWithHighestSales.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #
// MAGIC ---
// MAGIC # <p style="line-height: 2; font-size: 25px; font-weight: bold; letter-spacing: 2px; text-align: center;">What are the top 10 Publisher?
// MAGIC </p>

// COMMAND ----------

val topPublishers = cleanedData
  .groupBy("Publisher")
  .sum("Global_Sales")
  .orderBy(desc("sum(Global_Sales)"))
  .limit(10)

topPublishers.show()

// COMMAND ----------


