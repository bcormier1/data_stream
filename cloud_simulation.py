import socket
from pathlib import Path
import pandas
from analysis import Analysis
import datetime

CLOUD = 'C:\\Users\\Brodic Cromier\\Documents\\cloud_sim'
ADDRESS = ('localhost',9999)



if __name__ == '__main__':
    # set up socket to get connection
    server = socket.socket()

    server.bind(ADDRESS)

    server.listen(5)

    # main action loop the keep fielding requests
    while True:
        # reads data from files and puts it in a database for access later
        data = pandas.DataFrame()
        for file in Path(CLOUD).iterdir():
            location = CLOUD + '\\' + file.name
            if data.empty:
                data = Analysis.create_df(location)
            else:
                data = pandas.concat([data, Analysis.create_df(location)], ignore_index=True)

        conn, addr = server.accept()  # get a connection from a client

        request = conn.recv(1024)  # get and decode bytes request
        request = request.decode(encoding='utf-8')

        # turn request into usable date and time parts
        request = request.split(' ')
        req_date = request[0]
        t_request = request[1]

        t_request = t_request.split(':')
        req_time = datetime.time(int(t_request[0]), int(t_request[1]), int(t_request[2]))

        # grab data indexes that satisfy boolean conditions
        # if the date is on or above the request with also a valid time it is grabbed
        req_data = data[(data['Date'] >= req_date) | ((data['Date'] == req_date) & (data['Time'] >= req_time))]

        # convert grabbed DataFrame to json to transmit back to client
        json_request = req_data.to_json()

        # send all json data at once
        conn.sendall(json_request.encode(encoding='utf-8'))



