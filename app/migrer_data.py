import os
import time
import pandas as pd
from pymongo import MongoClient, UpdateOne
import sys
from datetime import datetime

def conversion(row):
    """Fonction de conversion des données"""
    return {
        "patient": {
            "Name": row["Name"],
            "Gender": row["Gender"],
            "Blood_Type": row["Blood Type"],
            "Age": row["Age"],
            "Medical_Condition": row["Medical Condition"],
            "Date_of_Admission": pd.to_datetime(row["Date of Admission"]),
            "Admission_Type": row["Admission Type"],
            "Discharge_Date": pd.to_datetime(row["Discharge Date"]),
            "Room_Number": row["Room Number"],
            "Doctor": row["Doctor"],
            "Hospital": row["Hospital"],
            "Billing_Amount": row["Billing Amount"],
            "Insurance_Provider": row["Insurance Provider"],
            "Medication": row["Medication"],
            "Test_Results": row["Test Results"]
        },
        "metadata": {
            "created_at": datetime.now(),
            "created_by": "migrator_service",
            "version": "1.0"
        }
    }

def migrate_in_batches(collection, cleaned_df, batch_size=1000):
    """Migration par lots - BEAUCOUP plus rapide"""
    total_migrated = 0
    total_rows = len(cleaned_df)
    
    print(f"🚀 Début migration par lots de {batch_size} documents")
    start_time = time.time()
    
    for i in range(0, total_rows, batch_size):
        batch_start = time.time()
        batch = cleaned_df.iloc[i:i+batch_size]
        operations = []
        
        # Préparer toutes les opérations du lot
        for _, row in batch.iterrows():
            query = {
                "patient.Name": row["Name"],
                "patient.Gender": row["Gender"],
                "patient.Blood_Type": row["Blood Type"],
                "patient.Age": row["Age"],
                "patient.Medical_Condition": row["Medical Condition"],
                "patient.Date_of_Admission": pd.to_datetime(row["Date of Admission"]),
                "patient.Admission_Type": row["Admission Type"],
                "patient.Discharge_Date": pd.to_datetime(row["Discharge Date"]),
                "patient.Room_Number": row["Room Number"],
                "patient.Doctor": row["Doctor"],
                "patient.Hospital": row["Hospital"],
                "patient.Billing_Amount": row["Billing Amount"],
                "patient.Insurance_Provider": row["Insurance Provider"],
                "patient.Medication": row["Medication"],
                "patient.Test_Results": row["Test Results"]
            }
            update = {"$set": conversion(row)}
            operations.append(UpdateOne(query, update, upsert=True))
        
        # Exécuter TOUT le lot d'un coup
        try:
            if operations:
                result = collection.bulk_write(operations, ordered=False)
                batch_migrated = result.upserted_count + result.modified_count
                total_migrated += batch_migrated
                
                batch_time = time.time() - batch_start
                progress = (i + len(batch)) / total_rows * 100
                
                print(f"📊 Lot {i//batch_size + 1}: {i + len(batch)}/{total_rows} "
                      f"({progress:.1f}%) - {batch_migrated} docs - {batch_time:.2f}s")
                
        except Exception as e:
            print(f"❌ Erreur lot {i//batch_size + 1}: {e}")
            # Continuer avec le lot suivant
            continue
    
    total_time = time.time() - start_time
    print(f"✅ Migration terminée: {total_migrated} docs en {total_time:.2f}s")
    return total_migrated

def wait_for_mongo(uri, max_retries=30):
    """Attendre MongoDB avec pool de connexions optimisé"""
    print(f"Connexion à MongoDB avec pool optimisé...")
    for i in range(max_retries):
        try:
            # Configuration optimisée pour les gros volumes
            client = MongoClient(
                uri, 
                serverSelectionTimeoutMS=10000,
                connectTimeoutMS=20000,
                socketTimeoutMS=60000,  # 60 secondes pour les grosses opérations
                maxPoolSize=50,         # Plus de connexions simultanées
                minPoolSize=10,
                maxIdleTimeMS=30000,
                waitQueueTimeoutMS=10000
            )
            client.admin.command('ping')
            print("✅ MongoDB connecté avec pool optimisé !")
            return client
        except Exception as e:
            print(f"⏳ Tentative {i+1}/{max_retries}: {e}")
            time.sleep(3)
    raise Exception("❌ Impossible de se connecter à MongoDB")

def main():
    try:
        # Connexion optimisée
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://user:pwuser@mongodb:27017/healthcare_db?authSource=healthcare_db')
        client = wait_for_mongo(mongo_uri)
        db = client['healthcare_db']
        collection = db['Patients']
        
        # Lecture et nettoyage
        print("📁 Lecture du fichier CSV...")
        df = pd.read_csv('data.csv')
        print(f"✅ CSV lu: {len(df)} lignes")
        
        # Nettoyage (utilisez votre fonction existante)
        try:
            from cleaning import clean_df
            cleaned_df = clean_df(df)
        except ImportError:
            print("⚠️ Module cleaning non trouvé, utilisation des données brutes")
            cleaned_df = df
        
        print(f"✅ Données préparées: {len(cleaned_df)} lignes")
        
        # Migration par lots (RAPIDE)
        migrated_count = migrate_in_batches(collection, cleaned_df, batch_size=1000)
        
        # Vérification finale
        total_docs = collection.count_documents({})
        print(f"📈 Total final: {total_docs} documents")
        print(f"🎉 Migration réussie: {migrated_count} documents migrés")
        
        return True
        
    except Exception as e:
        print(f"❌ ERREUR: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
