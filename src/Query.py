import os
import matplotlib.pyplot as plt
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, Row
import pyspark.sql.functions as F
import folium
from folium.plugins import HeatMapWithTime

os.environ['PYSPARK_PYTHON'] = '/home/bigdatalab24/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/home/bigdatalab24/anaconda3/bin/python'


class Query:
    def __init__(self):
        # Spark initialization and configuration
        sparkconf = SparkConf().setAppName('MyApp')
        sparkconf.set('spark.executor.memory', '10g')
        sparkconf.set('spark.executor.cores', '3')
        sparkconf.set('spark.cores.max', '3')
        sparkconf.set('spark.driver.memory', '10g')
        sparkconf.set('spark.driver.maxResultSize', '10g')
        self.sc = SparkContext(conf=sparkconf)
        self.spark = SparkSession.builder.config(conf=sparkconf).getOrCreate()
        filePath = '../data/HT1804/01/*/*.txt'
        data = self.sc.textFile(filePath)

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

    def average_speed_bar(self, ld=None, ru=None):
        '''
        A function return the average speed of taxi drivers in Shanghai
        through the whole day within designated geographical range.
        Look up through all the data if one of parameters is None (default).
        :param ld: a tuple of (longitude_left, latitude_down)
        :param ru: a tuple of (longitude_right, latitude_up)
        :return: (x, y) for drawing bars
        '''
        geo_range = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                   & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geo_range.select('speed', F.from_unixtime(F.unix_timestamp('time'), 'HH').alias('hour')).dropna()\
            .groupBy('hour').agg({'speed': 'mean'}).sort('hour').collect()
        x = list(range(24))
        y = [r['avg(speed)'] for r in data]
        return x, y

    @staticmethod
    def roundMap_speed(row):
        # round to 3 decimal places
        return (row.hour, round(float(row.longitude), 3), round(float(row.latitude), 3)), [row.speed, 1]

    @staticmethod
    def speedMap(data):
        (hour, lon, lat), speed = data
        speed = speed[0] / speed[1] / 30 / 100
        return hour, [lat, lon, speed]

    def average_speed_hotmap(self, ld=None, ru=None):
        '''
        Hot map version of average speed.
        :param ld:  a tuple of (longitude_left, latitude_down)
        :param ru:  a tuple of (longitude_right, latitude_up)
        :return: [[[latitude, longitude, average_speed]]] for each hour for each location
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                  & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geoRange.select('speed', 'longitude', 'latitude', F.from_unixtime(F.unix_timestamp('time'), 'HH').alias('hour'))\
                .dropna().rdd.map(self.roundMap_speed).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])\
                .map(self.speedMap).groupByKey().map(lambda x: list(x[1])).collect()
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

    @staticmethod
    def roundMap_passenger(row):
        # round to 3 decimal places
        return (row.hour, round(float(row.longitude), 3), round(float(row.latitude), 3)), [row.passenger, 1]

    @staticmethod
    def passengerMap(data):
        (hour, lon, lat), passenger = data
        passenger = passenger[0] / passenger[1] / 100
        return hour, [lat, lon, passenger]

    def average_occupy_hotmap(self, ld=None, ru=None):
        '''
        Hot map version of taxi occupy ratio.
        :param ld:  a tuple of (longitude_left, latitude_down)
        :param ru:  a tuple of (longitude_right, latitude_up)
        :return: [[[latitude, longitude, average_speed]]] for each hour for each location
        '''
        geoRange = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0])
                                  & (self.df.latitude >= ld[1]) & (self.df.latitude <= ru[1]))\
            if ld is not None and ru is not None else self.df
        data = geoRange.select('passenger', 'longitude', 'latitude', F.from_unixtime(F.unix_timestamp('time'), 'HH')
                 .alias('hour')).dropna().rdd.map(self.roundMap_passenger).reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])\
                 .map(self.passengerMap).groupByKey().map(lambda x: list(x[1])).collect()
        return data

    def plot_bar(self, x, y, label_x, label_y):
        '''
        Plot bar figure using matplotlib.
        :param x: iterable, data for x axis
        :param y: iterable, data for y axis
        :param label_x: str, label for x axis
        :param label_y: str, label for y axis
        :return:
        '''
        plt.bar(x, y)
        plt.xlabel(label_x)
        plt.ylabel(label_y)
        plt.show()

    def generage_hotmap(self, data):
        '''
        Generate dynamic hotmap with input data and save as a html file.
        :param data: a 3-d list of [[[latitude, longitude, average_speed]]] for each hour for each location
        :return: no return
        '''
        map_osm = folium.Map(location=[31.2234, 121.4814], zoom_start=10)
        HeatMapWithTime(data, radius=10).add_to(map_osm)
        map_osm.save('hotmap.html')


if __name__ == '__main__':
    # tests
    query = Query()
    # x, y =query.average_occupy_bar()
    # query.plot_bar(x, y, 'time: h', 'average occupy: ratio')
    data = query.average_speed_hotmap()
    query.generage_hotmap(data)
    del query
