# DataFrameApi
scala> val df = sqlContext.read.load("/user/hive/warehouse/os/000000_0")

1)No of athletes participated in each Olympic event
  Hive:
       SELECT count(Distinct a) from os;
  DataFrame ApI:
       scala> df.select("a").dropDuplicates().count()
	   Note:dropDuplicates is alias of Distinct in Sql.
2)No of medals each country won in each Olympic in ascending order
   Hive:
       SELECT c,d,sum(j) as Total FROM oc GROUP BY c,d order by total DESC;
   Dataframe:
       scala> df.groupBy("c","d").agg(sum("j").alias("Total")).sort($"Total".desc).show()
                                  (or)
       scala> df.groupBy("c","d").agg(sum("j").alias("Total")).orderBy(desc("Total")).show()

  	NOTE:
	agg:To use Alias(as) in data frame we have to use agg function.
	sort:Sort function is used to sort the data in desc or Asc. 
3)Top 10 athletes who won highest gold medalsin all the Olympic events
   Hive:
       select a,sum(g) as Goldmedal from os group by a order by goldmedal desc;
   DataFrame:
       scala>df.groupBy("a").agg(sum("g").alias("Goldmedal")).orderBy(desc("Goldmedal")).limit(10).show()

