import pandas
import time as timer
import socket
from analysis import Analysis
import datetime

ADDRESS = ('localhost', 9999)
WAITTIME = 3600*24  # amount of time to wait before checking again(in seconds)
# by adjusting the wait time the system can automaticaly check at intervals and increment time at the same rate
DATASTORE = 'C:\\Users\\Brodic Cromier\\Documents\\data_stream\\data_store.pickle'  # directory to store pickle in



def check_server(address, date):
    # create client side socket
    checker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # create the proper request string with a helper function
    request = Analysis.date_to_request(date)

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




def increment_time(current_date):
    current_date += datetime.timedelta(seconds=WAITTIME)  # increment time by specified amount of seconds
    return current_date


def store(data, date):
    start_date = date - datetime.timedelta(weeks=1)  # get the date from a week ago

    week_ago = Analysis.date_to_str(start_date)
    current_time = pandas.Timestamp(date.hour, date.minute, date.second)  # time is the same a week ago as now


    # store data from within a week in a pickle
    data[(data['Date'] > week_ago) | ((data['Date'] == week_ago) & (data['Time'] > current_time))].to_pickle(DATASTORE)


def load_data():
    return pandas.read_pickle(DATASTORE)  # return dataframe from pickle



if __name__ == '__main__':
    # initialize empty objects just for use in conditionals
    data = pandas.DataFrame()
    analysis = list()

    date = datetime.datetime(2016, 7, 6)

    while True:
        timer.sleep(5)  # repeat loop every n seconds

        # contact server and normalize response data
        server_response = check_server(ADDRESS, date)
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

        store(data, date)

        data = load_data()

        date = increment_time(date)  # increments time/date for automatic checking
