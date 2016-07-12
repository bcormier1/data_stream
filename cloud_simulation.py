import socket
from pathlib import Path
import pandas
from analysis import Analysis
import datetime

CLOUD = 'C:\\Users\\Brodic Cromier\\Documents\\cloud_sim'
ADDRESS = ('localhost',9999)



if __name__ == '__main__':
    server = socket.socket()

    server.bind(ADDRESS)

    server.listen(5)

    while True:
        data = pandas.DataFrame()
        for file in Path(CLOUD).iterdir():
            location = CLOUD + '\\' + file.name
            if data.empty:
                data = Analysis.create_df(location)
            else:
                data = pandas.concat([data, Analysis.create_df(location)], ignore_index=True)

        conn, addr = server.accept()

        request = conn.recv(1024)
        request = request.decode(encoding='utf-8').split('/')

        req_date = request[2]+request[1]+request[0]
        req_data = data.loc[data['Date'] >= req_date]

        json_request = req_data.to_json()

        conn.send(json_request.encode(encoding='utf-8'))



