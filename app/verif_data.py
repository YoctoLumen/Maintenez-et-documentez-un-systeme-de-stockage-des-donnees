from pymongo import MongoClient

def PostMigration(client, db, collection):
    # Vérifier le nombre total de documents
    total_docs = collection.count_documents({})
    print(f"Nombre total de documents dans la collection : {total_docs}")

    # Vérifier la présence de certains champs dans un échantillon
    sample_doc = collection.find_one()
    if sample_doc:
        print("\nExemple de document dans la collection :")
        for key, value in sample_doc.items():
            print(f" - {key} : {value}")

    # Vérifier l'absence de doublons sur un champ unique, par exemple 'patient.name' (si unique)
    pipeline = [
        {"$group": {"_id": "$patient.Name", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 1}}}
    ]
    doublons = list(collection.aggregate(pipeline))
    if doublons:
        print("\nDoublons détectés (sur 'patient.Name') :")
    else:
        print("\nAucun doublon détecté sur 'patient.Name'.")

    # Vérifier les valeurs manquantes dans certains champs
    fields_to_check = ["patient.Name", "patient.Gender", "patient.Blood_Type"]
    for field in fields_to_check:
        missing_count = collection.count_documents({field: {"$exists": False}})
        print(f"\nNombre de documents sans le champ '{field}': {missing_count}")
