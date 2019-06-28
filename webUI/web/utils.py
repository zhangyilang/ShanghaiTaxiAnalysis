import datetime
import matplotlib.pyplot as plt
plt.switch_backend('agg')
import folium
from folium.plugins import HeatMapWithTime
from pyspark.mllib.regression import LabeledPoint


def processTime(line):
    """
    a function for processing the date time data
    :param line:
    :return:
    """
    line[10] = float(line[10])
    line[11] = float(line[11])
    t = line[9]
    try:
        t = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
    except:
        t = None
    if t != None:
        # clean the data
        t = str(t).split(' ')
        if t[0] != '2018-04-01':
            t[0] = '2018-04-01'
        t = t[0] + ' ' + t[1]
        t = t = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
    line[9] = t
    return line


# 下客记录
def findOff(vec):
    """
    find the record of a customoer get off the taxi
    and how long the taxi takes to wait until the next customer
    vec: a vector of (time,lon,lat,if empty)
    """
    l = len(vec)
    i = 0
    flag = 0
    re = []
    # find the first time when the taxi is not empty
    while i < l:
        if vec[i][3] == 0:
            flag = 1
            i = i + 1
        elif flag == 1 and vec[i][3] == 1:
            # a customer off record
            lon_t = vec[i][1]
            lat_t = vec[i][2]
            time_t = vec[i][0]
            while i < l:
                if vec[i][3] == 1:
                    i = i + 1
                else:
                    break
            if i >= l:
                break
            else:
                wait_t = vec[i][0] - time_t
            re.append(((lon_t,lat_t),time_t,wait_t))
            flag = 0
        else:
            i = i + 1
    return re


def findOn(vec):
    """
    find the record of a customoer get off the taxi
    and how long the taxi takes to wait until the next customer
    vec: a vector of (time,lon,lat,if empty)
    """
    l = len(vec)
    i = 0
    flag = 0
    re = []
    # find the first time when the taxi is not empty
    while i < l:
        if vec[i][3] == 1:
            flag = 1
            i = i + 1
        elif flag == 1 and vec[i][3] == 0:
            # a customer on record
            lon_t = vec[i][1]
            lat_t = vec[i][2]
            time_t = vec[i][0]
            while i < l:
                if vec[i][3] == 0:
                    i = i + 1
                else:
                    break
            if i >= l:
                break
            else:
                wait_t = vec[i][0] - time_t
                lon_e = vec[i][1]
                lat_e = vec[i][2]

            re.append(((lon_t, lat_t), time_t, wait_t, (lon_e, lat_e)))
            flag = 0
        else:
            i = i + 1
    return re


def datetime2minute(dt):
    """
    A function for transfer 1 datetime.datetime instance to another datetime.time
    but with second = 0
    e.g.: datetime2minute(datetime.datetime())
    """
    return datetime.datetime(dt.year,dt.month,dt.day,dt.hour,dt.minute,0)


def datetime2hour(dt):
    """
    A function for transfer 1 datetime.datetime instance to another datetime.time
    but with minute and second = 0
    e.g.: datetime2hour(datetime.datetime())
    """
    return datetime.datetime(dt.year, dt.month, dt.day, dt.hour, 0, 0)


def intensityMap(line):
    """
    a functiom to map one record to ((hour,coordinate),1)
    :param line: a record in rdd
    :return:
    """
    hour = datetime2hour(line[9])
    lon = round(line[10], 3)
    lat = round(line[11], 3)
    # coordinate = (lon, lat)
    return (hour, lon, lat), 1


def roundMap_speed(row):
    # round to 3 decimal places
    return (row.hour, round(float(row.longitude), 3), round(float(row.latitude), 3)), [row.speed, 1]


def speedMap(data):
    (hour, lon, lat), speed = data
    speed = speed[0] / speed[1] / 30 / 100
    return hour, [lat, lon, speed]


def roundMap_passenger(row):
    # round to 3 decimal places
    return (row.hour, round(float(row.longitude), 3), round(float(row.latitude), 3)), [row.passenger, 1]


def passengerMap(data):
    (hour, lon, lat), passenger = data
    passenger = passenger[0] / passenger[1] / 100
    return hour, [lat, lon, passenger]


def tourTimeMap(record):
    car, upTime, upCoord, downCoord = record[0], record[1][1], record[1][0], record[1][3]
    hour, minute = upTime[-8:-3].split(':')
    label = record[1][2].seconds
    features = [car, float(hour)+float(minute)/60, upCoord[0], upCoord[1], downCoord[0], downCoord[1]]
    return LabeledPoint(label, features)


def plot_curve(x, y, label_x, label_y, name):
    '''
    Plot broken line curve with matplotlib in granularity of minute.
    :param x: iterable, data for x axis
    :param y: iterable, data for y axis
    :param label_x: str, label for x axis
    :param label_y: str, label for y axis
    :param name: a str
    :return:
    '''
    plt.plot(x, y)
    plt.xlabel(label_x)
    plt.ylabel(label_y)
    plt.show()
    fig = plt.gcf()
    fig.savefig(name)


def plot_bar(x, y, label_x, label_y, name):
    '''
    Plot bar figure with matplotlib in granularity of hour.
    :param x: iterable, data for x axis
    :param y: iterable, data for y axis
    :param label_x: str, label for x axis
    :param label_y: str, label for y axis
    :param name: a str
    :return:
    '''
    plt.bar(x, y)
    plt.xlabel(label_x)
    plt.ylabel(label_y)
    plt.show()
    fig = plt.gcf()
    fig.savefig(name)


def generate_hotmap(data, name):
    '''
    Generate dynamic hotmap with input data and save as a html file.
    :param data: a 3-d list of [[[latitude, longitude, average_speed]]] for each hour for each location
    :param name: a str
    :return: no return
    '''
    map_osm = folium.Map(location=[31.2234, 121.4814], zoom_start=10)
    HeatMapWithTime(data, radius=10).add_to(map_osm)
    map_osm.save(name)
