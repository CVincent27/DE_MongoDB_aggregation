from load.load_mongo import get_mongo_collection
from datetime import datetime
import json
import os

collection = get_mongo_collection()

# * Changez le gender 0 en gender 1
def update_gender_zero():
    result = collection.update_many(
        {"gender": 0},
        {"$set": {"gender": 1}}
    )
    print(f"\n{result.modified_count} doc modifiés.")

def get_most_frequent_trips_female():
    pipeline = [
    {"$match": {"gender": 1}},
    {"$group": {
        "_id": {
            "start_station_name": "$start_station_name",
            "end_station_name": "$end_station_name"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$limit": 5}
    ]

    results = list(collection.aggregate(pipeline))
    print("\nTop 5 trajets les plus empruntés par des femmes :")
    #print(results)
    for res in results:
        start = res['_id']['start_station_name']
        end = res['_id']['end_station_name']
        count = res['count']
        print(f"De {start} à {end} : {count} trajets")
    print("\n")

# * Quel est le nombre total de trajets par type d’utilisateur (Subscriber vs Customer) pour le premier jour de l'année ?
def nb_travels_by_type():
    pipeline = [
        {
            "$match": {
                "start_time": {
                    #$eq ne rfonctionne pas à cause des heures, min...)
                    "$gte": datetime.strptime("2016-01-01", "%Y-%m-%d"),
                    "$lt": datetime.strptime("2016-01-02", "%Y-%m-%d")
                }
            }
        },
        {
            "$group": {
                "_id": "$usertype",
                "count": {"$sum": 1}
            }
        },
        {
            "$sort": {"count": -1}
        }
    ]
    results = list(collection.aggregate(pipeline))
    for result in results:
        print(f"type: {result['_id']}, nb travel: {result['count']} le 01/01/2016")
    return results

# * Quelle est la durée moyenne des trajets par station de départ pour les trajets commençant entre 7h et 9h ?
def avg_duration_by_station_morning():
    pipeline = [
        {"$addFields": {
            "start_hour": { "$hour": "$start_time" }
        }},
        {"$match": {
            "start_hour": { "$gte": 7, "$lt": 9 }
        }},
        {"$group": {
            "_id": "$start_station_name",
            "average_duration": { "$avg": "$tripduration" }
        }},
        {"$sort": { "average_duration": -1 }}
    ]
    results = list(collection.aggregate(pipeline))

    # formatage resultat en dict
    formatted_results = [
        {
            "station": res['_id'],
            "average_duration_minutes": round(res['average_duration'] / 60, 2)
        }
        for res in results
    ]

    json_file_path = os.path.join(os.path.dirname(__file__), 'avg_duration_by_station_morning.json')
        
    with open(json_file_path, 'w') as json_file:
        json.dump(formatted_results, json_file, indent=4)
    print("\nJSON avg_duration_by_station_morning crée")
    return json_file_path

# * Quel est le top 3 des stations avec la plus forte fréquentation de prise de location, entre 6h et 8h ?
def top_3_start_stations_morning():
    pipeline = [
        {"$addFields": {
            "start_hour": { "$hour": "$start_time" }
        }},
        {"$match": {
            "start_hour": { "$gte": 6, "$lt": 8 }
        }},
        {"$group": {
            "_id": "$start_station_name",
            "count": { "$sum": 1 }
        }},
        {"$sort": { "count": -1 }},
        {"$limit": 3}
    ]
    results = list(collection.aggregate(pipeline))

    print("\nTop 3 des stations avec la plus forte prise de location entre 6h et 8h :")
    for res in results:
        station = res["_id"]
        count = res["count"]
        print(f"{station} : {count} départs")

    return results

# * Quelle est la durée médiane des trajets pour les + de 65 ans ?
# pipeline en cours de construction
def median_duration_over_65():
    pipeline = [
        {
            "$addFields": {
                "age": {
                    "$subtract": [
                        { "$year": "$$NOW" },
                        "$birth_year"
                    ]
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "birth_year": 1,
                "calculated_year": { "$year": "$$NOW" },
                "age": 1
            }
        },
    ]
    results = list(collection.aggregate(pipeline))

    unique_ages = set()
    # print("\nCheck du calcul d'âge :")
    # for res in results:
    #     birth = res.get("birth_year")
    #     age = res.get("age")
    #     print(f"Naissance: {birth} | Âge: {age}")
    #     unique_ages.add(age)

    return sorted(unique_ages)

# * Quelle est la répartition des trajets (nombre de trajets) par tranche horaire de 2 heures (faire visualisation, 0h-2h, 2h-4h etc..) ?
# pipeline en cours de construction
def nb_travel_by_two_hours():
    pipeline = [
        {
            "$addFields": {
                "start_hour": { "$hour": "$start_time" }
            }
        },
        {
            "$group": {
                "_id": {
                    "$subtract": [
                        { "$divide": ["$start_hour", 2] },
                        { "$mod": [{ "$divide": ["$start_hour", 2] }, 1] }
                    ]
                },
                "count": { "$sum": 1 }
            }
        },
        {
            "$sort": { "_id": 1 }
        }
    ]
    results = list(collection.aggregate(pipeline))

    # json_file_path = os.path.join(os.path.dirname(__file__), 'nb_travel_by_two_hours.json')
        
    # with open(json_file_path, 'w') as json_file:
    #     json.dump(results, json_file, indent=4)
    # print("JSON nb travail by two hours crée")
    # return json_file_path

    #visualisation à faire