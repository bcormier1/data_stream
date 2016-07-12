import pandas
import os
import time as timer
import socket
from analysis import Analysis
import datetime

ADDRESS = ('localhost',9999)






def check_server(address,date):
    checker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    try:
        checker.connect(address)
        checker.send('07/07/2016'.encode(encoding='utf-8'))

        json_data = checker.recv(10000).decode(encoding='utf-8')

        return pandas.read_json(json_data)
    finally:
        checker.close()





if __name__ == '__main__':
    data = pandas.DataFrame()
    analysis = list()

    date = datetime.datetime(2016, 7, 6)

    while True:
        timer.sleep(5)
        server_response = check_server(ADDRESS, date)
        if not server_response.empty:
            if data.empty:
                data = server_response
            else:
                data = pandas.concat([data, server_response], ignore_index=True)

            data = data.sort_values(by=['Date', 'Time'])
            data.index = range(len(data))
            data['Time'] = pandas.to_datetime(data['Time'])

            analysis = Analysis.analyze(data, 'Temp', lambda x: x > 100)
            print(Analysis.convert_to_str(analysis))

            date = datetime.datetime(date.year, date.month, date.day + 1)
