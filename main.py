import json
from bson import json_util
import pandas as pd
import os


from cleaning.check_missing_values import data_type, missing_values, find_duplicates, drop_duplicates
from cleaning.clean_data import clean_keys
from cleaning.clean_empty_values import replace_empty_strings
from load.load_mongo import drop_collection, insert_data, get_mongo_collection

from analysis.suspicious_date.suspicious_date import mark_overlaps
from analysis.check_age.check_age import check_age
from analysis.missing_birth_year.missing_birth import check_missing_birth_year
from analysis.suspicious_duration.suspicious_duration import check_duration_one_second, check_suspicious_duration, find_inconsistent_trip_durations
from analysis.check_gender.check_gender import check_gender

from questions_metiers.mongodb_queries import update_gender_zero, get_most_frequent_trips_female, nb_travels_by_type, avg_duration_by_station_morning, top_3_start_stations_morning, median_duration_over_65, nb_travel_by_two_hours

json_path = 'data/trips_.json'

# conversion BSON -> Python natif
with open(json_path, 'r') as file:
    raw_data = [json.loads(line, object_hook=json_util.object_hook) for line in file]

# Nettoyage clés et valeurs vides
cleaned_keys = [clean_keys(doc) for doc in raw_data]
cleaned_data = [replace_empty_strings(doc) for doc in cleaned_keys]

# Convertir en DataFrame une fois
df = pd.DataFrame(cleaned_data)

# Analyse
data_type(df)
missing_values(df)
find_duplicates(df)
df = drop_duplicates(df)

df.to_json("data/cleaned_data.json", orient="records", lines=True)

# Conversion finale avant insertion
cleaned_data = df.to_dict(orient='records')

# 1ere Insertion en base avant enquete
collection = get_mongo_collection()
drop_collection(collection)
insert_data(collection, cleaned_data)

# Enquêtes

## Dates suspicieuses
# group_keys (https://stackoverflow.com/questions/38856583/what-does-the-group-keys-argument-to-pandas-groupby-actually-do)
df = df.groupby('bikeid').apply(mark_overlaps)
#print(df[df['suspicious_date']])

## Utilisateur trop jeune (< 13 ans)
df = check_age(df)

## Birth year manquant
df = check_missing_birth_year(df)

## Duration 1s et durée suspicieuse
df = check_duration_one_second(df)
df = check_suspicious_duration(df)
find_inconsistent_trip_durations(df)

# Check 0 gender
df = check_gender(df)

updated_cleaned_data = df.to_dict(orient='records')

# update base
drop_collection(collection)
insert_data(collection, updated_cleaned_data, label="(Mise à jour de la base après enquête)")   

## Questions métiers
update_gender_zero()
get_most_frequent_trips_female()
nb_travels_by_type()
avg_duration_by_station_morning()
top_3_start_stations_morning()
#median_duration_over_65() # pas encore fonctionnel
#nb_travel_by_two_hours() # pas encore fonctionnel
