
import matplotlib.pyplot as plt
plt.switch_backend('agg')

def plot (x,y):
    plt.plot(x,y)
    plt.savefig("./web/static/images/test1.png")
    return 0