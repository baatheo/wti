import time
import requests


def main():
    #test1()
    test()


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
    listOfRequests = []

    y1 = requests.get(url + 'ratings')
    listOfRequests.append(y1)
    time.sleep(0.01)
    y2 = requests.get(url + 'avg-genre-ratings/all-users')
    listOfRequests.append(y2)
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