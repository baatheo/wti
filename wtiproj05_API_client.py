import json
import time
import requests
import matplotlib.pyplot as plt
import numpy as np
from wtiproj05_api_logic import api
from wtiproj05_redis_client import Queue


def main():
    startFlask = time.clock()
    print(startFlask)
    test1()
    flaskTime = time.clock() - startFlask
    print(flaskTime)
    startCherry = time.clock()
    print(startCherry)
    test()
    cherryTime = time.clock() - startCherry
    print(cherryTime)
    print(flaskTime, " : ", cherryTime)
    labels = ['FLASK', 'CHERRYPY']
    values = [flaskTime, cherryTime]
    x = np.arange(len(labels))  # the label locations
    print(x)
    width = 0.35
    fig, ax = plt.subplots()
    rects = ax.bar(x - width / 2, values, width)

    ax.set_title('Scores by group and gender')
    ax.set_xticklabels(labels)
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
    ax.legend()
    plt.show()


def test1():
    url = 'http://127.0.0.1:9123/'
    myRecord = {"userID": 68, "movieID": 9123, "rating": 4.5, "genre": "Children"}
    listOfRequests = []

    y1 = requests.get(url + 'ratings')
    listOfRequests.append(y1)
    time.sleep(0.01)
    y2 = requests.get(url + 'avg-genre-ratings/all-users')
    listOfRequests.append(y2)
    time.sleep(0.01)
    y3 = requests.get(url + 'avg-genre-ratings/user/68')
    listOfRequests.append(y3)
    time.sleep(0.01)
    x = requests.post(url + 'rating', data=myRecord)
    listOfRequests.append(x)
    time.sleep(0.01)
    y2 = requests.get(url + 'avg-genre-ratings/all-users')
    listOfRequests.append(y2)
    time.sleep(0.01)
    y3 = requests.get(url + 'avg-genre-ratings/user/68')
    listOfRequests.append(y3)
    time.sleep(0.01)
    y4 = requests.get(url + 'profile/postAll')
    listOfRequests.append(y4)
    time.sleep(0.01)
    y5 = requests.get(url + 'profile/all')
    listOfRequests.append(y5)
    time.sleep(0.01)
    y6 = requests.get(url + 'profile/78')
    listOfRequests.append(y6)
    time.sleep(0.01)
    for i in listOfRequests:
        print("request url: " + str(i.url))
        print("request status_code: " + str(i.status_code))
        print("request headers: " + str(i.headers))
        print("request text: " + str(i.text))
        print("request request.headers: " + str(i.request.headers))
        print('-------------------------------------------------')
        print('-------------------------------------------------')


def test():
    url = 'http://127.0.0.1:9898/'
    myRecord = {"userID": 68, "movieID": 9123, "rating": 4.5, "genre": "Children"}
    listOfRequests = []

    y1 = requests.get(url + 'ratings')
    listOfRequests.append(y1)
    time.sleep(0.01)
    y2 = requests.get(url + 'avg-genre-ratings/all-users')
    listOfRequests.append(y2)
    time.sleep(0.01)
    y3 = requests.get(url + 'avg-genre-ratings/user/68')
    listOfRequests.append(y3)
    time.sleep(0.01)
    x = requests.post(url + 'rating', data=myRecord)
    listOfRequests.append(x)
    time.sleep(0.01)
    y2 = requests.get(url + 'avg-genre-ratings/all-users')
    listOfRequests.append(y2)
    time.sleep(0.01)
    y3 = requests.get(url + 'avg-genre-ratings/user/68')
    listOfRequests.append(y3)
    time.sleep(0.01)
    y4 = requests.get(url + 'profile/postAll')
    listOfRequests.append(y4)
    time.sleep(0.01)
    y5 = requests.get(url + 'profile/all')
    listOfRequests.append(y5)
    time.sleep(0.01)
    y6 = requests.get(url + 'profile/78')
    listOfRequests.append(y6)
    time.sleep(0.01)
    for i in listOfRequests:
        print("request url: " + str(i.url))
        print("request status_code: " + str(i.status_code))
        print("request headers: " + str(i.headers))
        print("request text: " + str(i.text))
        print("request request.headers: " + str(i.request.headers))
        print('-------------------------------------------------')
        print('-------------------------------------------------')


if __name__ == '__main__':
    main()
