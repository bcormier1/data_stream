import pandas
from datetime import datetime


class Analysis:

    @staticmethod
    def analyze(data, field, condition):
        # takes data and checks condition for all items in the field
        LENGTHTHRESHOLD = 2

        over_groups = list()
        time_range_over = list()
        for index in range(len(data)):
            if condition(data.loc[index][field]):
                time_range_over.append(index)
            else:
                if len(time_range_over) > LENGTHTHRESHOLD:
                    startD = data.loc[time_range_over[0]]['Date']
                    startT = data.loc[time_range_over[0]]['Time']
                    start = datetime(startD.year, startD.month, startD.day, startT.hour, startT.minute, startT.second,
                                     startT.microsecond)

                    endD = data.loc[time_range_over[-1]]['Date']
                    endT = data.loc[time_range_over[-1]]['Time']
                    end = datetime(endD.year, endD.month, endD.day, endT.hour, endT.minute, endT.second,
                                   endT.microsecond)

                    over_groups.append(((data.loc[time_range_over[0]]['Date'], data.loc[time_range_over[0]]['Time']),
                                        (data.loc[time_range_over[-1]]['Date'], data.loc[time_range_over[-1]]['Time']),
                                        (end - start).total_seconds() / len(time_range_over)))

                time_range_over = list()
        return over_groups

    @staticmethod
    def create_df(file):
        # can be changed to access files of different types or locations
        return pandas.read_excel(file)


    @staticmethod
    def convert_to_str(groups: list) -> list:
        # converts the result of analyze to an easily readable string
        readable = list()
        for item in groups:
            start_d = item[0][0]
            start_t = item[0][1]
            end_d = item[1][0]
            end_t = item[1][1]

            readable.append(((str(start_d.month) + '/' + str(start_d.day) + '/' + str(start_d.year),
                              str(start_t.hour) + ':' + str(start_t.minute) + ':' + str(start_t.second)),
                             (str(end_d.month) + '/' + str(end_d.day) + '/' + str(end_d.year),
                              str(end_t.hour) + ':' + str(end_t.minute) + ':' + str(end_t.second))))
        return readable

    @staticmethod
    def date_to_request(date, time):
        # takes date and time and puts it in the proper form to index the data
        if date.month < 10:
            month = '0' + str(date.month)
        else:
            month = str(date.month)
        if date.day < 10:
            day = '0' + str(date.day)
        else:
            day = str(date.day)
        date_str = str(date.year) + month + day

        if time.hour < 10:
            hour = '0' + str(time.hour)
        else:
            hour = str(time.hour)
        if time.minute < 10:
            minute = '0' + str(time.minute)
        else:
            minute = str(time.minute)
        if time.second < 10:
            second = '0' + str(time.second)
        else:
            second = str(time.second)
        time_str = hour + ':' + minute + ':' + second

        return date_str + ' ' + time_str
