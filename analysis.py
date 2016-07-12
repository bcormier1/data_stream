import pandas
from datetime import datetime


class Analysis:

    @staticmethod
    def analyze(data, field, condition):
        LENGTHTHRESHOLD = 1

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
    def date_to_str(date: datetime)-> str:
        #takes datetime and converts it to a string to be used to index the database
        if date.month < 10:
            month_str = '0' + str(date.month)
        else:
            month_str = str(date.month)

        if date.day < 10:
            day_str = '0' + str(date.day)
        else:
            day_str = str(date.day)

        return str(date.year) + month_str + day_str
