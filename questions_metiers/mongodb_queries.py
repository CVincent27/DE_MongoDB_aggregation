from load.load_mongo import get_mongo_collection
from datetime import datetime

collection = get_mongo_collection()

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
        print(f"\ntype: {result['_id']}, nb travel: {result['count']} le 01/01/2016")
    return results

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

    print("\nDurée moy trajets entre 7h et 9h par station:")
    for res in results:
        station = res['_id']
        # transfo minutes
        avg_dur = round(res['average_duration'] / 60, 2) 
        print(f"{station} : {avg_dur} min")

    return results

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

    print("\nCheck du calcul d'âge :")
    for res in results:
        birth = res.get("birth_year")
        age = res.get("age")
        print(f"Naissance: {birth} | Âge: {age}")
        unique_ages.add(age)

    print("\nListe unique des âges :", sorted(unique_ages))
    return sorted(unique_ages)