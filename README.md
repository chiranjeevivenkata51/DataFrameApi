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
4)No of athletes who won gold and whose age is less than 20
    Hive:
        select count(a) from os where b<20 and g>=1;
   DataFrame:
        scala> df.where(($"b"<20)&&($"g">=1)).count()
     Note:Where is same where clause in sql and hive
8)
hive> select c,sum(j) as vi from em1 where  f="Wrestling" and d =2012 group by c order by vi desc;

DataFrame:
scala> df.filter($"d"===2012 && $"f"==="Wrestling").groupBy("c").agg(sum("j") alias ("vi")).sort($"vi".desc).show(200)

create table ct1(c1 string,c2 string,c3 string,c4 string,c5 string,c6 string,c7 string,c8 string,c9 string,c10 string,c11 string)ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",")  
