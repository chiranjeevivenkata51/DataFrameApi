# DataFrameApi
scala> val df = sqlContext.read.load("/user/hive/warehouse/os/000000_0")

1)No of athletes participated in each Olympic event
  Hive:
       SELECT count(Distinct a) from os;
  DataFrame ApI:
       scala> df.select("a").dropDuplicates().count()
	   Note:dropDuplicates is alias of Distinct in Sql.

        
