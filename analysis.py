class Analysis:
    
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