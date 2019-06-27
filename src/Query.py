import os
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
import pyspark.sql.functions as F
import datetime
from utils import *
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import numpy as np
from pyspark.ml.feature import OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator


os.environ['PYSPARK_PYTHON'] = '/home/bigdatalab24/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/bigdatalab24/anaconda3/bin/python'

default_Start = datetime.datetime.strptime('2018-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')
default_End = datetime.datetime.strptime('2018-04-01 23:59:59', '%Y-%m-%d %H:%M:%S')


class Query:
    def __init__(self):
        # initialize some useful param
        self.default_Start = datetime.datetime.strptime('2018-04-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        self.default_End = datetime.datetime.strptime('2018-04-01 23:59:59', '%Y-%m-%d %H:%M:%S')
        self.downRecord = None
        self.upRecord = None
        self.model = None

        # Spark initialization and configuration
        sparkconf = SparkConf().setAppName('MyApp')
        sparkconf.set('spark.executor.memory', '10g')
        sparkconf.set('spark.executor.cores', '3')
        sparkconf.set('spark.cores.max', '3')
        sparkconf.set('spark.driver.memory', '10g')
        sparkconf.set('spark.driver.maxResultSize', '10g')
        self.sc = SparkContext(conf=sparkconf)
        self.spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()
        filePath = '/home/bigdatalab24/BigScalePJ/data/HT1804/01/*/*.txt'
        data = self.sc.textFile(filePath)
        self.data = data
        self.data = self.data.map(lambda x: x.split('|'))
        # Process the time data
        self.data = self.data.map(processTime).filter(lambda x: x[9] is not None)

        # Creat dataframe
        parts = data.map(lambda x: x.split("|"))
        rows = parts.map(lambda p: Row(car=p[0], control=p[1], business=p[2], passenger=int(p[3]), light=p[4],
                                       road=p[5], brake=p[6], date=p[8], time=p[9],
                                       longitude=float(p[10]), latitude=float(p[11]), speed=float(p[12]),
                                       direction=float(p[13]), satellite=p[14]))
        self.df = self.spark.createDataFrame(rows)
        self.df.createOrReplaceTempView("DF")
        print("DataFrame created")
        # df.show(5)

    def __del__(self):
        # Stop spark context and spark session
        self.sc.stop()
        self.spark.stop()

    def init_updown(self):
        """
        A initialization function for up records and down records
        may take quite some time
        :return: None
        """
        # self.data = self.data.map(lambda x: x.split('|'))
        # Process the time data
        # self.data = self.data.map(processTime).filter(lambda x: x[9] != None)
        # may take a lot time here
        self.data = self.data.sortBy(lambda x: x[9])
        # self.downRecord store a data frame of up records
        self.downRecord = self.data.map(
            lambda line: (line[0], (line[9], float(line[10]), float(line[11]), int(line[3])))).groupByKey().map(
            lambda x: (x[0], list(x[1]))).mapValues(findOff).flatMap(lambda line: [(line[0], h) for h in line[1]])
        # downrows = downRecord.map(lambda p: Row(car=p[0], Coordinate=p[1][0], downTime=p[1][1],
                                                #Gap=p[1][2].days * 3600 * 24 + p[1][2].seconds))
        # self.downRecordDF = self.spark.createDataFrame(downrows)
        self.upRecord = self.data.map(lambda line: (line[0], (line[9], float(line[10]), float(line[11]), int(line[3]))))\
                        .groupByKey().map(lambda x: (x[0], list(x[1]))).mapValues(findOn)\
                        .flatMap(lambda line: [(line[0], h) for h in line[1]])

    def GetAverageWaitTime(self, StartTime=default_Start, EndTime=default_End, dl=(0, 0), ur=(180, 90)):
        """
        StartTime: beginning time, a datetime.datetime instance
        EndTime: ending time, a datetime.datetime instance
        dl: the coordinate of the leftdown point
        ur: the coordinate of the upright point
        return the average taxi waiting time during this period and in this field
        """
        if self.downRecord == None:
            print('please initialize down and up records first!')
            return None
        dl_lon, dl_lat = dl
        ur_lon, ur_lat = ur
        re = self.downRecord.filter(
            lambda x: x[1][1] > StartTime and x[1][1] <= EndTime and x[1][0][0] >= dl_lon and x[1][0][0] <= ur_lon and
                      x[1][0][1] >= dl_lat and x[1][0][1] <= ur_lat).map(lambda line: (line[1][2], 1)).reduce(
            lambda a, b: (a[0] + b[0], a[1] + b[1]))
        re = re[0] / re[1]
        return re.days * 3600 * 24 + re.seconds

    def AverageWaitTimePlot(self,StartTime=default_Start, EndTime=default_End, dl=(0, 0), ur=(180, 90)):
        """
        A function to make a plot of average waiting time in each minute
        """
        if self.downRecord is None:
            print('please initialize down and up records first!')
            return None
        dl_lon, dl_lat = dl
        ur_lon, ur_lat = ur
        re = self.downRecord.filter(
            lambda x: x[1][1] > StartTime and x[1][1] <= EndTime and x[1][0][0] >= dl_lon and x[1][0][0] <= ur_lon and
                      x[1][0][1] >= dl_lat and x[1][0][1] <= ur_lat).map(
            lambda line: (datetime2minute(line[1][1]), (line[1][2], 1))).reduceByKey(
            lambda a, b: (a[0] + b[0], a[1] + b[1])).sortBy(lambda x: x[0]).collect()
        times = [t[0] for t in re]
        wait_times = [t[1][0].total_seconds() / (t[1][1] + 0.0001) for t in re]
        # print(times)
        # print(wait_times)
        plt.plot(times, wait_times)
        plt.xlabel("Time")
        plt.ylabel("Mean Waiting Time(s)")
        plt.show()

    def drawOneCar(self, carNo, path='/home/bigdatalab24/1.png'):
        """
        draw a plot of the track of car carNo in a day
        :param carNo: the number of target car
        :param path: the png path to store your image
        :return: None
        """
        result = self.GetCarTrack(carNo=carNo)
        lons = [h[10] for h in result]
        lats = [h[11] for h in result]
        plt.figure(figsize=(15, 15))
        map = Basemap(projection='merc', resolution='h', area_thresh=0.1, llcrnrlon=121.05, llcrnrlat=31.002,
                      urcrnrlon=121.9128, urcrnrlat=31.4466)
        map.drawcoastlines()
        map.drawcountries()
        # map.fillcontinents(color='mistyrose',lake_color='slategrey')
        map.drawlsmask(land_color='slategray', ocean_color='slategray', lakes=True)
        # map.drawrivers(color = 'slategrey',linewidth=0.3)
        # map.drawmapboundary(fill_color='slategray')
        # lons = [h[10] for h in temp]
        # lats = [h[11] for h in temp]
        lon, lat = map(np.array(lons), np.array(lats))
        map.scatter(lon, lat, color='blue', s=0.01, alpha=1)
        fig = plt.gcf()
        fig.savefig(path)
        plt.show()

    def drawAllCars(self, i=0):
        """
        A function to draw a plot of all cars in Shanghai in 1 hour!
        :param i:the target hour accept 0 ~ 23
        :return: None
        """
        if i < 0 or i > 23:
            print('not a valid hour!')
            return None
        s = '2018-04-01 ' + str(i).zfill(2) + ':00:00'
        e = '2018-04-02 ' + '00' + ':00:00'
        start = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
        end = datetime.datetime.strptime(e, '%Y-%m-%d %H:%M:%S')
        result = self.data.filter(lambda x: x[9] >= start and x[9] <= end).collect()
        lons = [h[10] for h in result]
        lats = [h[11] for h in result]
        plt.figure(figsize=(15, 15))
        map = Basemap(projection='merc', resolution='h', area_thresh=0.1, llcrnrlon=121.05, llcrnrlat=31.002,
                      urcrnrlon=121.9128, urcrnrlat=31.4466)
        map.drawcoastlines()
        map.drawcountries()
        # map.fillcontinents(color='mistyrose',lake_color='slategrey')
        map.drawlsmask(land_color='slategray', ocean_color='slategray', lakes=True)
        # map.drawrivers(color = 'slategrey',linewidth=0.3)
        # map.drawmapboundary(fill_color='slategray')
        # lons = [h[10] for h in temp]
        # lats = [h[11] for h in temp]
        lon, lat = map(np.array(lons), np.array(lats))
        map.scatter(lon, lat, color='red', s=0.0001, alpha=0.1)
        fig = plt.gcf()
        path = '/home/bigdatalab24/' + str(i) + '.png'
        fig.savefig(path)
        plt.show()

    def GetCarTrack(self,carNo, Start=default_Start, End=default_End):
        """
        carNo: the car number
        Start: Start time, a datetime instance
        End: End time, a datetime instance
        """
        result = self.data.filter(lambda x: x[0] == str(carNo) and x[9] >= Start and x[9] <= End).collect()
        return result

    def carHotmap(self):
        """
        a function to return data for car intensity hot map
        :return: 3-order list
        """
        norm = 10000
        data = self.data.map(intensityMap).reduceByKey(lambda a, b: a + b).map(lambda x: (x[0][0], (x[0][2], x[0][1], x[1] / norm)))\
               .groupByKey().map(lambda x: [list(h) for h in x[1]]).collect()
        # print(self.data.map(intensityMap).reduceByKey(lambda a,b: a + b).reduce(lambda a,b:a[1] > b[1] and a or b))
        return data

    def average_speed_bar(self, ld=None, ru=None):
        '''
        A function return the average speed of taxi drivers in Shanghai
        through the whole day within designated geographical range.
        Look up through all the data if one of parameters is None (default).
        Granularity: hour.
        :param ld: a tuple of (longitude_left, latitude_down)
        :param ru: a tuple of (longitude_right, latitude_up)
        :return: (x, y) for drawing bars.
        '''
        geo_range = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                   & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geo_range.select('speed', F.from_unixtime(F.unix_timestamp('time'), 'HH').alias('hour')).dropna()\
            .groupBy('hour').agg({'speed': 'mean'}).sort('hour').collect()
        x = list(range(24))
        y = [r['avg(speed)'] for r in data]
        return x, y

    def average_speed_curve(self, ld=None, ru=None):
        '''
        A function return the average speed of taxi drivers in Shanghai
        through the whole day within designated geographical range.
        Look up through all the data if one of parameters is None (default).
        Granularity: minute.
        :param ld: a tuple of (longitude_left, latitude_down)
        :param ru: a tuple of (longitude_right, latitude_up)
        :return: (x, y) for drawing curve
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                   & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geoRange.select('speed', F.from_unixtime(F.unix_timestamp('time'), 'HH:mm').alias('minute')).dropna()\
               .groupBy('minute').agg({'speed': 'mean'}).sort('minute').collect()
        x = list(range(24 * 60))
        y = [r['avg(speed)'] for r in data]
        return x, y

    def average_speed_hotmap(self, ld=None, ru=None):
        '''
        Hot map version of average speed.
        :param ld:  a tuple of (longitude_left, latitude_down)
        :param ru:  a tuple of (longitude_right, latitude_up)
        :return: [[[latitude, longitude, average_speed]]] for each location for each hour
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                  & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geoRange.select('speed', 'longitude', 'latitude', F.from_unixtime(F.unix_timestamp('time'), 'HH').alias('hour'))\
                .dropna().rdd.map(roundMap_speed).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])\
                .map(speedMap).groupByKey().map(lambda x: list(x[1])).collect()
        return data

    def average_occupy_bar(self, ld=None, ru=None):
        '''
        A function return the average occupied rate of taxis in Shanghai
        through the whole day within designated geographical range.
        Look up through all the data if one of parameters is None (default).
        :param ld: a tuple of (longitude_left, latitude_down)
        :param ru: a tuple of (longitude_right, latitude_up)
        :return: (x, y) for drawing bars
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0]) & (self.df.latitude >= ld[1])\
                   & (self.df.latitude <= ru[1])) if ld is not None and ru is not None else self.df
        data = geoRange.select('passenger', F.from_unixtime(F.unix_timestamp('time'), 'HH').alias('hour'))\
                 .dropna().groupBy('hour').agg({'passenger': 'mean'}).sort('hour').collect()
        x = list(range(24))
        y = [r['avg(passenger)'] for r in data]
        return x, y

    def average_occupy_curve(self, ld=None, ru=None):
        '''
        A function return the average occupied rate of taxis in Shanghai
        through the whole day within designated geographical range.
        Look up through all the data if one of parameters is None (default).
        :param ld: a tuple of (longitude_left, latitude_down)
        :param ru: a tuple of (longitude_right, latitude_up)
        :return: (x, y) for drawing bars
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0]) & (self.df.latitude >= ld[1])\
                   & (self.df.latitude <= ru[1])) if ld is not None and ru is not None else self.df
        data = geoRange.select('passenger', F.from_unixtime(F.unix_timestamp('time'), 'HH:mm').alias('minute'))\
                 .dropna().groupBy('minute').agg({'passenger': 'mean'}).sort('minute').collect()
        x = list(range(24 * 60))
        y = [r['avg(passenger)'] for r in data]
        return x, y

    def average_occupy_hotmap(self, ld=None, ru=None):
        '''
        Hot map version of taxi occupy ratio.
        :param ld:  a tuple of (longitude_left, latitude_down)
        :param ru:  a tuple of (longitude_right, latitude_up)
        :return: [[[latitude, longitude, average_speed]]] for each location for each hour
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                  & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geoRange.select('passenger', 'longitude', 'latitude', F.from_unixtime(F.unix_timestamp('time'), 'HH')
                 .alias('hour')).dropna().rdd.map(roundMap_passenger).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])\
                 .map(passengerMap).groupByKey().map(lambda x: list(x[1])).collect()
        return data

    def tourTimePredict_train(self):
        '''
        Train a ridge regression model which could predict the tour time given boarding coordinate, getting off
         coordinate and boarding time.
        '''
        # The high 4 numbers of upCoord is longitude and the low 3 numbers is latitude
        onRecordDF = self.upRecord.filter(lambda p: (p[1][0][0] >= 120.5) & (p[1][0][0] <= 122.1) &
                                                    (p[1][0][1] >= 30.4) & (p[1][0][1] <= 31.5))\
                         .map(lambda p: Row(upCoord=int((round(p[1][0][0], 1) - 120.5) * 10 * 12 + (round(p[1][0][1], 1) - 30.4) * 10),
                                            upTime=p[1][1].hour, duration=p[1][2].days * 60 * 24 + p[1][2].seconds/60,
                                            manhLon=abs(p[1][3][0] - p[1][0][0]), manhLat=abs(p[1][3][1] - p[1][0][1])))
        onRecordDF = self.spark.createDataFrame(onRecordDF)

        # generate feature vector
        encoder_time = OneHotEncoder(inputCol='upTime', outputCol='upTime_onehot', dropLast=False)
        encoder_coord = OneHotEncoder(inputCol='upCoord', outputCol='upCoord_onehot', dropLast=False)
        assembler = VectorAssembler(inputCols=['upTime_onehot', 'upCoord_onehot', 'manhLon', 'manhLat'], outputCol='features')
        onRecordDF = assembler.transform(encoder_coord.transform(encoder_time.transform(onRecordDF)))
        trainSet, validSet, testSet = onRecordDF.randomSplit([7., 1., 2.])

        # train model
        lr = LinearRegression(labelCol='duration', regParam=0.01, maxIter=100)
        self.model = lr.fit(trainSet)
        train_summary = self.model.summary
        print('RMSE of training:', train_summary.rootMeanSquaredError, 'min')
        print('Adjusted R2 of training:', train_summary.r2adj)

        # evaluation
        evaluator = RegressionEvaluator(labelCol='duration')
        model_valid = self.model.transform(validSet)
        print('RMSE on validation set:', evaluator.evaluate(model_valid, {evaluator.metricName: 'rmse'}), 'min')
        model_valid = self.model.transform(testSet)
        print('RMSE on test set:', evaluator.evaluate(model_valid, {evaluator.metricName: 'rmse'}), 'min')

    def tourTimePredict(self, data):
        '''
        Predict the tour time given data of boarding coordinate, getting off coordinate and boarding time.
        :param data: [upCoord_onehot, manh_lon, manh_lat, upTime_onehot]
        :return: prediction of board time of the tour.
        '''
        test = self.spark.createDataFrame([data], ['features'])
        return self.model.transform(test).head().prediction


if __name__ == '__main__':
    # tests
    query = Query()
    x, y = query.average_speed_curve()
    plot_curve(x, y, 'time: minute', 'average speed', name='average_speed_curve')

    x, y = query.average_speed_bar()
    plot_bar(x, y, 'time: h', 'average speed: ratio', name='average_speed_bar')

    data = query.average_speed_hotmap()
    generate_hotmap(data, name='average_speed_hotmap')

    x, y = query.average_occupy_curve()
    plot_curve(x, y, 'time: h', 'average occupied rate', name='average_occupy_curve')

    x, y = query.average_occupy_bar()
    plot_bar(x, y, 'time: h', 'average occupy: ratio', name='average_occupy_bar')

    data = query.average_occupy_hotmap()
    generate_hotmap(data, name='average_occupy_hotmap')

    data = query.carHotmap()
    generate_hotmap(data, name='car_hotmap')
