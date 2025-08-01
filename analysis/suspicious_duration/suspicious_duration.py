import pandas as pd

def check_duration_one_second(df):
    count_one_second_duration = 0
    for index, row in df.iterrows():
        trip_duration = row['tripduration']
        if trip_duration == 1:
            count_one_second_duration += 1
            df.at[index, 'one_second_trip_duration'] = True
        else:
            df.at[index, 'one_second_trip_duration'] = False
    print(f"\nnb trajet de 1 second: {count_one_second_duration}")
    return df

def check_suspicious_duration(df):
    df['suspicious_duration'] = df['start_time'] == df['stop_time']
    df['impossible_duration'] = df['stop_time'] < df['start_time']

    count_suspicious_duration = df['suspicious_duration'].sum()
    count_impossible_duration = df['impossible_duration'].sum()

    print(f"Nb trajet durée suspecte: {count_suspicious_duration}")
    print(f"Nb trajet durée négative : {count_impossible_duration}")

    return df

#ancienne version
#def check_suspicious_duration(df):
    count_suspicious_duration = 0
    for index, row in df.iterrows():
        start_time = row['start_time']
        stop_time = row['stop_time']
        if start_time == stop_time:
            count_suspicious_duration += 1
            df.at[index, 'suspicious_duration'] = True
        elif stop_time > start_time:
            df.at[index, 'impossible_duration'] = True
        else:
            df.at[index, 'suspicious_duration'] = False
    print(f"Nombre de trajets suspects : {count_suspicious_duration}")
    return df