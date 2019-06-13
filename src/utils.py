import datetime
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

#downRecord = data.map(lambda line:(line[0],(line[9],float(line[10]),float(line[11]),int(line[3])))).groupByKey().map(lambda x:(x[0],list(x[1]))).mapValues(findOff).flatMap(lambda line:[(line[0],h) for h in line[1]])