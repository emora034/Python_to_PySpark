

from datetime import datetime
#initialize timer for serial process, inclusive of all data cleaning and transformations
start_time = datetime.now()

import pandas as pd
data0 = pd.read_csv('tripdata_2017-01.csv', ',')
data1= pd.read_csv('tripdata_2017-02.csv', ',') 
import numpy as np

#combining the data sets for the 2 months
data = pd.concat([data0, data1], ignore_index=True)

data.tpep_pickup_datetime.dtype
data.tpep_dropoff_datetime.dtype

#convert string to a datetime
data['tpep_pickup_datetime'] = pd.to_datetime(data['tpep_pickup_datetime'])
data['tpep_dropoff_datetime'] = pd.to_datetime(data['tpep_dropoff_datetime'])


#add column with diff values in minutes
data['time_diff'] = round(((data['tpep_dropoff_datetime'] - data['tpep_pickup_datetime']).astype('timedelta64[s]'))/60.,2)

max(data.time_diff)
data.head(n=10)

#create a speed variable in miles per hour
data['speed_MPH']= round((data.trip_distance/(data.time_diff/60.)),1)
data.head(n=10)

#some of the reported data is wrong. the below data point reports 22.1 miles in 0.02 min.
#which yields a speed of 663000mph
data.loc[data['speed_MPH'].argmax()]


#remove the inf values due to 0 time division
data['speed_MPH'] = data['speed_MPH'].replace(np.inf, 0)



data['d'] = np.where(data['trip_distance']>=0.98,2,1)
data['d'] = np.where(data['trip_distance']>=1.61,3,data['d'])
data['d'] = np.where(data['trip_distance']>=3.00,4,data['d'])

print('The number of rows/lines in out "Taxi" dataset is: ',len(data))



########## Data Analysis Starts Here ################
#Initialize timer for serial process data analysis portion
start_time2 = datetime.now()


#####################################################
#####################################################
############# Distance Ranges vs. tips ##############
## First: Relation between the distance and the tip_amount
## We only take the payment_type: 2 that corresponds to credit card


temp1 = data.groupby('d', as_index=False)['total_amount'].mean()
#print(temp1)

credit_card = data['payment_type']==1
data2=data[credit_card]
#data2.head(n=10)

temp3 = data2.groupby('d', as_index=False)['tip_amount'].mean()
#print(temp3)


##################################################
##################################################
############ fare_amount vs RateCodeID ###########


#fare_amount vs RateCodeID
temp = data.groupby('RatecodeID', as_index=False)['fare_amount'].mean()
#print(temp)


##################################################
##################################################
############ Trip Duration vs RateCodeID ###########
trip=data.groupby('RatecodeID', as_index=False)['time_diff'].mean()
#print(trip)


##################################################
##################################################
############ Speed vs RateCodeID ###########
vel=data.groupby('RatecodeID', as_index=False)['speed_MPH'].mean()
#print(vel)


#get current time to calculate processing times
end_time = datetime.now()
end_time2 = datetime.now()

print('The Serial processing time of all data tyding, processing and analysis is: {}'.format(end_time - start_time))
print('The Serial processing time of the data analysis only is: {}'.format(end_time2 - start_time2))



###########################
########## PLOTS ##########
#initialize timer for plots only
start_time3 = datetime.now()

label= 'Standard rate', 'JFK', 'Newark', 'Westchester', 'Negotiated fare', 'Group ride', 'other'
import plotly.express as px

fig=px.bar(trip, y='time_diff', x=label, color=label, title='Rate Code vs Trip Duration (mins.' )
fig.show()


figv=px.bar(vel, y='speed_MPH', x=label, color=label, title='Rate Code vs Speed (mph )')
figv.show()


fig = px.pie(temp, values='fare_amount', names=label, title='Rate Code vs the time-and-distance fare amount')
fig.show()


fig = px.bar(temp, y='fare_amount', x=label,color=label, title='Rate Code vs the time-and-distance fare amount')
fig.show()


labels1= 'less than 0.98 miles', 'between 0.98 and 1.61 miles', 'between 1.61 and 3.00 miles', 'more than 3.00 miles'
y='$ 1,40','$ 1.69','$ 2.21','$ 4.91'
fig = px.bar(temp3, x=labels1, y='tip_amount', color=labels1, text=y)
fig.show()

end_time3 = datetime.now()
print('The Serial processing time of the plots is: {}'.format(end_time3 - start_time3))
