# Serial and Parallel Processing - From Python to Pyspark

In this repository you will find a simple example of code translation from Python to Pyspark, along with serial and parallel processing implementations - the latter using RDD. 

You may find more information on RDD at https://spark.apache.org/docs/latest/rdd-programming-guide.html.

The dataset used for these examples contained the yellow and green 2017 taxi trip records (January & February), including fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The data was collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP). The trip data was not created by the TLC, and TLC makes no representations as to the accuracy of these data. You can find more information of the dataset in the following link: 
https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

If you have issues fetching the datasets, feel free to contact me. I'll be happy share them with you as I wasn't able to upload them given the size restrictions - 50+MB

To analyze the data, we chose four diﬀerent studies of interest:

1. The average fare amount (the time-and-distance fare calculated by the meter) for each of the rate codes: standard rate, JFK, Newark, Nassau or Westchester, negotiated fare and group ride.

2. The average trip duration given the reported rate code.

3. The average speed by recorded rate code.

4. The average total amount of payment by the trip distance given speciﬁed mileage intervals: i) 0 to 0.98 miles, ii) 0.98 to 1.61, iii) 1.61 to 3 miles and iv) more than 3 miles.

5. The trip distance and average tip amount with the same intervals as previously mentioned.

The study is ﬁrst carried out in Jupiter notebook, with the Python language and is later translated into Pyspark to be processed in parallel.

1. Fare Amount for each Rate Code 

  • The following graphs were made with the Python (Figure 1) and the Pyspark (Figure 2) programs. We can see that the highest fare amount belongs to the people that go to Newark with an average of about 64.31 dollars, while the lowest fare amount belongs to the group ride, followed by the standard rate.

