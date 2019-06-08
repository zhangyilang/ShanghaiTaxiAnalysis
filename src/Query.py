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
        geo_range = self.df.filter((self.df.longitude >= ld[0]) & (self.df.longitude <= ru[0]) & (self.df.latitude >= ld[1])
                             & (self.df.latitude <= ru[1])) if ld is not None and ru is not None else self.df
        result = geo_range.select('speed', F.from_unixtime(F.unix_timestamp('time'), 'HH').alias('hour')).dropna()\
            .groupBy('hour').agg({'speed': 'mean'}).sort('hour').collect()
        x = list(range(24))
        y = [r['avg(speed)'] for r in result]
        return x, y

    def plot_bar(self, x, y):
        plt.bar(x, y)
        plt.xlabel('time: h')
        plt.ylabel('average speed: km/h')
        plt.show()


if __name__ == '__main__':
    # tests
    query = Query()
    x, y = query.average_speed_bar()
    query.plot_bar(x, y)
