from django.shortcuts import render
#import Query as Q
import matplotlib.pyplot as plt
plt.switch_backend('agg')

# Create your views here.
from django.shortcuts import HttpResponse

def index(request):


    return render(request, 'home.html')

def index2(request):

    return render(request, 'onecar.html')

def index3(request):

    return render(request, 'allcar.html')

def index4(request):

    return render(request, 'speed.html')

def index5(request):

    return render(request, 'occupy.html')

def index6(request):

    return render(request, 'predict.html')

def index7(request):

    return render(request, 'average_speed_map.html')

def index8(request):

    return render(request, 'average_occupy_map.html')




def i2o(request):
    id = request.POST['id']

    # Q.query.drawOneCar(id, 'one_car.png')

    return render(request, 'onecar_out.html')

def i3o(request):


    return render(request, 'allcar_out.html')

def i4o(request):
    l1 = request.POST['l1']
    l2 = request.POST['l2']
    r1 = request.POST['r1']
    r2 = request.POST['r2']
    leftdown=(l1,l2)
    upright=(r1,r2)

    # x, y = Q.query.average_speed_curve(leftdown, upright)
    # Q.plot_curve(x, y, 'time: minute', 'average speed', name='./web/static/images/average_speed_curve.png')
    #
    # x, y = Q.query.average_speed_bar(leftdown, upright)
    # Q.plot_bar(x, y, 'time: h', 'average speed: ratio', name='./web/static/images/average_speed_bar.png')
    #
    # data = Q.query.average_speed_hotmap(leftdown, upright)
    # Q.generate_hotmap(data, name='average_speed_hotmap.html')
    return render(request, 'speed_out.html')



def i5o1(request):


    return render(request, 'average_speed_hotmap.html')

def i5o(request):
    l1 = request.POST['l1']
    l2 = request.POST['l2']
    r1 = request.POST['r1']
    r2 = request.POST['r2']
    leftdown = (l1, l2)
    upright = (r1, r2)

    # x, y = Q.query.average_occupy_curve(leftdown, upright)
    # plot_curve(x, y, 'time: h', 'average occupied rate', name='./web/static/images/average_occupy_curve.png')
    #
    # x, y = Q.query.average_occupy_bar(leftdown, upright)
    # plot_bar(x, y, 'time: h', 'average occupy: ratio', name='./web/static/images/average_occupy_bar.png')
    #
    # data = Q.query.average_occupy_hotmap(leftdown, upright)
    # Q.generate_hotmap(data, name='average_occupy_hotmap.html')

    return render(request, 'occupy_out.html')

def i6o(request):
    # be = request.POST['be']
    # end = request.POST['end']
    # time = request.POST['time']
    # data=(be,end,time)
    # pre=Q.tourTimePredict(data)
    # predict={'b':data}

    return render(request, 'predict_out.html',predict)