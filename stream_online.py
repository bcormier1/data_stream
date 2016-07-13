import pandas
import time as timer
import socket
from analysis import Analysis
import datetime

ADDRESS = ('localhost', 9999)



def check_server(address, date, time):
    # create client side socket
    checker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # create the proper request string with a helper function
    request = Analysis.date_to_request(date, time)

    # inside a try in case the connection fails the exception can be handled differently to prevent data loss
    # its not necessary currently but could be if things are added to it
    try:
        # connect to server and send request
        checker.connect(address)
        checker.send(request.encode(encoding='utf-8'))

        json_data = checker.recv(10000).decode(encoding='utf-8')  # get response from server and decode

        return pandas.read_json(json_data)  # return
    finally:
        checker.close()  # close connection no matter what


def increment_date(date):
    # returns a new datetime object one day ahead
    day = date.day
    month = date.month
    year = date.year

    day += 1

    if day == 30 and month in (4, 6, 9, 11):
        day = 1
        month += 1
    elif day == 31 and month in (1, 3, 5, 7, 8, 10, 12):
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
        day = 1
    elif day == 28 and month == 2 and ((year % 4 == 0 and year % 100 != 0) or year % 400 == 0):  # leap year
        day = 1
        month += 1
    elif day == 29 and month == 2:  # not a leap year
        day = 1
        month += 1

    return datetime.datetime(year, month, day)



if __name__ == '__main__':
    # initialize empty objects just for use in conditionals
    data = pandas.DataFrame()
    analysis = list()

    date = datetime.datetime(2016, 7, 6)

    time = datetime.time(0, 0, 0)

    while True:
        timer.sleep(5)  # repeat loop every 5 seconds

        # contact server and normalize response data
        server_response = check_server(ADDRESS, date, time)
        server_response = server_response.sort_values(by=['Date', 'Time'])  # sort in ascending order based on time
        server_response.index = range(len(server_response))  # restart index at 0
        server_response['Time'] = pandas.to_datetime(server_response['Time'])  # make the time column datetime objects

        if not server_response.empty:  # if we get a response
            # if we either have no data collected or already have the same set don't repeat analysis
            if data.empty or not server_response.equals(data):
                # add response to local database
                if data.empty:
                    data = server_response
                else:
                    data = pandas.concat([data, server_response], ignore_index=True)

                # normalize data
                data = data.sort_values(by=['Date', 'Time'])
                data['Time'] = pandas.to_datetime(data['Time'])
                data = data.drop_duplicates()  # no fear of losing data because it is time series
                data.index = range(len(data))

                analysis = Analysis.analyze(data, 'Temp', lambda x: x > 100)  # send data to be analyzed
                print(Analysis.convert_to_str(analysis))  # print easy to read time frames that met the condition

        date = increment_date(date)  # increments date for automatic checking
