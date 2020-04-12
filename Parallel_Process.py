##########	RDD	##########
import os
import sys
spark_path = r"C:\User\username\file\Spark\spark-2.4.5-bin-hadoop2.7" # spark installed folder
os.environ['SPARK_HOME'] = spark_path
sys.path.insert(0, spark_path + "/bin")
sys.path.insert(0, spark_path + "/python/pyspark/")
sys.path.insert(0, spark_path + "/python/lib/pyspark.zip")
sys.path.insert(0, spark_path + "/python/lib/py4j-0.10.7-src.zip")
#PYSPARK_DRIVER_PYTHON=jupyter./bin/pyspark



#initialize spark
import findspark
findspark.init(r"C:\Users\emora\OneDrive\Desktop\Spark\spark-2.4.5-bin-hadoop2.7")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


spark = SparkSession.builder.appName("taxis").master("local[*]").getOrCreate()


from datetime import datetime
#initialize time var. to calculate processing time of all data tyding and analysis
start_time = datetime.now()

data1=spark.read.csv("tripdata_2017-01.csv",header="true", inferSchema="true")
data2=spark.read.csv("tripdata_2017-02.csv",header="true", inferSchema="true")

#concat data                                  
data = data1.union(data2)
                                  
                                    
#data.limit(10).toPandas()
num_lines = data.count()


print ("Number of rows in our Taxi dataset:", num_lineas)

#Variables transformations

from pyspark.sql import functions as F
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import to_timestamp

data=data.withColumn('tpep_pickup_datetime',to_timestamp(data.tpep_pickup_datetime, "yyyy-MM-dd HH:mm:ss"))
data=data.withColumn('tpep_dropoff_datetime',to_timestamp(data.tpep_dropoff_datetime, "yyyy-MM-dd HH:mm:ss"))

data=data.withColumn('tpep_pickup_datetime', unix_timestamp('tpep_pickup_datetime'))
data=data.withColumn('tpep_dropoff_datetime', unix_timestamp('tpep_dropoff_datetime'))

#compute the trip duration and speed, rounded to 2 decimal places
data= data.withColumn('diff', F.round(((F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))/60.),2))

data=data.withColumn('speed_MPH', F.round((data.trip_distance/(data.diff/60.)),1))

#replace null values with 0
data=data.fillna(0)

from operator import add
from pyspark.sql.functions import when, col

#define thresholds
data = data.withColumn('d', when(col('trip_distance') <= 0.98, 1).when(col('trip_distance') <= 1.61, 2).when(col('trip_distance')<=3.00,3).when(col('trip_distance')>3.00,4))

credit_card = data['payment_type']==1
#print(type(credit_card))
data2=data[credit_card]
#print(data)

##########################
##########################
#### Paralell Process ####
### Change data to RDD ###

data=data.orderBy('RatecodeID').rdd
data2=data2.orderBy('d').rdd

data.getNumPartitions()
data2.getNumPartitions()


########## Data Analysis Starts Here ################
#initialize "timer"
start_time2 = datetime.now()

########### RDD Trip Duration#############
avg_diff=data.map(lambda x:[x.RatecodeID, x.diff])\
    .mapValues(lambda c:(c,1))\
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
    .mapValues(lambda c:c[0]/c[1])

#avg_diff.collect()
#print(avg_diff)

######### RDD Speed ###################
avg_speed=data.map(lambda x:[x.RatecodeID, x.speed_MPH])\
    .mapValues(lambda c:(c,1))\
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
    .mapValues(lambda c:c[0]/c[1])
#print(avg_speed)
#avg_speed.collect()

########### RDD FARE ##############
fare=data.map(lambda x:[x.RatecodeID, x.fare_amount])\
    .mapValues(lambda c:(c,1))\
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
    .mapValues(lambda c:c[0]/c[1])
    
  
#print(fare)
#fare.collect()


########### RDD TOTAL AMOUNT ##############
avg_total=data.map(lambda x:[x.d, x.total_amount])\
    .mapValues(lambda c:(c,1))\
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
    .mapValues(lambda c:c[0]/c[1])
#print(avg_total)
#avg_total.collect()


########### RDD TIP AMOUNT ##############
avg_tip=data2.map(lambda x:[x.d, x.tip_amount])\
    .mapValues(lambda c:(c,1))\
    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
    .mapValues(lambda c:c[0]/c[1])
#print(avg_tip)
#avg_tip.collect()

end_time = datetime.now()
end_time2 = datetime.now()
print('Parallel program duration: {}'.format(end_time - start_time))
print('Parallel program data analysis duration: {}'.format(end_time2 - start_time2))

############################
########### PLOTS ##########

start_time3 = datetime.now()

label= 'Standard rate', 'Other','JFK', 'Newark', 'Westchester', 'Negotiated fare', 'Group ride'

import matplotlib.pyplot as plt

df2= avg_diff.toDF()

y1 = [val._1 for val in df2.select('_1').collect()]

plt.bar(label, y1)
plt.xticks(rotation=45)
plt.show()

dfs=avg_speed.toDF()
ysp=[val._2 for val in dfs.select('_2').collect()]

plt.bar(label, ysp)
plt.xticks(rotation=45)
plt.show()

df=fare.toDF()
y_ans_val = [val._2 for val in df.select('_2').collect()]

plt.bar(label, y_ans_val)
plt.xticks(rotation=45)
plt.show()


df3= avg_total.toDF()
y3 = 'less than 0.98 miles', 'between 0.98 and 1.61', 'between 1.61 and 3.00 miles', 'more than 3.00 miles'

y4 = [val._2 for val in df3.select('_2').collect()]
plt.bar(y3, y4)
plt.xticks(rotation=45)
plt.show()


df4= avg_tip.toDF()
y5 = [val._2 for val in df4.select('_2').collect()]

plt.bar(y3, y5)
plt.xticks(rotation=45)
plt.show()

end_time3 = datetime.now()
print('Parallel program plots processing time is: {}'.format(end_time3 - start_time3))


spark.stop()