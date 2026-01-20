import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import sys

class MigrationVerifier:
    def __init__(self, mongo_uri, csv_file):
        self.mongo_uri = mongo_uri
        self.csv_file = csv_file
        self.client = None
        self.db = None
        self.collection = None
        self.errors = []
        self.warnings = []
        
    def connect_to_mongo(self):
        """Connexion à MongoDB"""
        try:
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client['healthcare_db']
            self.collection = self.db['Patients']
            print("✅ Connexion à MongoDB réussie")
            return True
        except Exception as e:
            print(f"❌ Erreur de connexion à MongoDB : {e}")
            return False
    
    def verify_document_count(self):
        """Vérifier le nombre de documents"""
        print("\n🔍 VÉRIFICATION DU NOMBRE DE DOCUMENTS")
        
        # Compter les documents MongoDB
        mongo_count = self.collection.count_documents({})
        print(f"Documents dans MongoDB : {mongo_count}")
        
        # Compter les lignes CSV
        try:
            df = pd.read_csv(self.csv_file)
            csv_count = len(df)
            print(f"Lignes dans le CSV : {csv_count}")
            
            # Vérification
            if mongo_count == csv_count:
                print("✅ Nombre de documents correct")
                return True
            else:
                error_msg = f"❌ Différence de count : MongoDB={mongo_count}, CSV={csv_count}"
                print(error_msg)
                self.errors.append(error_msg)
                return False
        except Exception as e:
            error_msg = f"❌ Erreur lecture CSV : {e}"
            print(error_msg)
            self.errors.append(error_msg)
            return False
    
    def verify_data_structure(self):
        """Vérifier la structure des données"""
        print("\n🔍 VÉRIFICATION DE LA STRUCTURE")
        
        # Vérifier qu'il y a au moins un document
        sample_doc = self.collection.find_one()
        if not sample_doc:
            error_msg = "❌ Aucun document trouvé"
            print(error_msg)
            self.errors.append(error_msg)
            return False
        
        # Vérifier la structure attendue
        required_fields = [
            'patient.Name', 'patient.Age', 'patient.Gender',
            'patient.Medical_Condition', 'patient.Date_of_Admission',
            'patient.Discharge_Date', 'patient.Hospital'
        ]
        
        missing_fields = []
        for field in required_fields:
            keys = field.split('.')
            current = sample_doc
            try:
                for key in keys:
                    current = current[key]
            except (KeyError, TypeError):
                missing_fields.append(field)
        
        if missing_fields:
            error_msg = f"❌ Champs manquants : {missing_fields}"
            print(error_msg)
            self.errors.append(error_msg)
            return False
        else:
            print("✅ Structure des données correcte")
            return True
    
    def verify_data_types(self):
        """Vérifier les types de données"""
        print("\n🔍 VÉRIFICATION DES TYPES DE DONNÉES")
        
        # Vérifier les types sur un échantillon
        pipeline = [
            {"$limit": 100},
            {"$project": {
                "name_type": {"$type": "$patient.Name"},
                "age_type": {"$type": "$patient.Age"},
                "admission_type": {"$type": "$patient.Date_of_Admission"},
                "billing_type": {"$type": "$patient.Billing_Amount"}
            }}
        ]
        
        sample = list(self.collection.aggregate(pipeline))
        
        # Vérifications des types
        type_errors = []
        
        for doc in sample[:5]:  # Vérifier les 5 premiers
            if doc.get('name_type') != 'string':
                type_errors.append(f"Name devrait être string, trouvé: {doc.get('name_type')}")
            if doc.get('age_type') not in ['int', 'long', 'double']:
                type_errors.append(f"Age devrait être numérique, trouvé: {doc.get('age_type')}")
            if doc.get('admission_type') != 'date':
                type_errors.append(f"Date_of_Admission devrait être date, trouvé: {doc.get('admission_type')}")
        
        if type_errors:
            for error in type_errors:
                print(f"❌ {error}")
                self.errors.append(error)
            return False
        else:
            print("✅ Types de données corrects")
            return True
    
    def verify_data_integrity(self):
        """Vérifier l'intégrité des données"""
        print("\n🔍 VÉRIFICATION DE L'INTÉGRITÉ")
        
        # 1. Vérifier les valeurs nulles
        null_names = self.collection.count_documents({"patient.Name": {"$in": [None, ""]}})
        null_ages = self.collection.count_documents({"patient.Age": None})
        
        if null_names > 0:
            warning_msg = f"⚠️ {null_names} documents avec nom vide"
            print(warning_msg)
            self.warnings.append(warning_msg)
        
        if null_ages > 0:
            warning_msg = f"⚠️ {null_ages} documents avec âge null"
            print(warning_msg)
            self.warnings.append(warning_msg)
        
        # 2. Vérifier les valeurs aberrantes
        invalid_ages = self.collection.count_documents({
            "$or": [
                {"patient.Age": {"$lt": 0}},
                {"patient.Age": {"$gt": 120}}
            ]
        })
        
        if invalid_ages > 0:
            warning_msg = f"⚠️ {invalid_ages} documents avec âge aberrant"
            print(warning_msg)
            self.warnings.append(warning_msg)
        
        # 3. Vérifier la cohérence des dates
        invalid_dates = self.collection.count_documents({
            "$expr": {"$gt": ["$patient.Date_of_Admission", "$patient.Discharge_Date"]}
        })
        
        if invalid_dates > 0:
            warning_msg = f"⚠️ {invalid_dates} documents avec dates incohérentes"
            print(warning_msg)
            self.warnings.append(warning_msg)
        
        print("✅ Vérification d'intégrité terminée")
        return True
    
    def verify_duplicates(self):
        """Vérifier les doublons"""
        print("\n🔍 VÉRIFICATION DES DOUBLONS")
        
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "name": "$patient.Name",
                        "gender":"$patient.Gender",
                        "blood": "$patient.Blood_Type",
                        "age": "$patient.Age",
                        "condition": "$patient.Medical_Condition",
                        "admission": "$patient.Date_of_Admission",
                        "discharge": "$patient.Discharge_Date",
                        "room": "$patient.Room_Number",
                        "doctor": "$patient.Doctor",
                        "hospital": "$patient.Hospital",
                        "billing": "$patient.Billing_Amount",
                        "insurance": "$patient.Insurance_Provider",
                        "medication": "$patient.Medication",
                        "results": "$patient.Test_Results"
                    },
                    "count": {"$sum": 1}
                }
            },
            {"$match": {"count": {"$gt": 1}}},
            {"$count": "duplicates"}
        ]
        
        result = list(self.collection.aggregate(pipeline))
        duplicate_count = result[0]['duplicates'] if result else 0
        
        if duplicate_count > 0:
            warning_msg = f"⚠️ {duplicate_count} groupes de doublons détectés"
            print(warning_msg)
            self.warnings.append(warning_msg)
        else:
            print("✅ Aucun doublon détecté")
        
        return True
    
    def generate_statistics(self):
        """Générer des statistiques"""
        print("\n📊 STATISTIQUES DE MIGRATION")
        
        total_docs = self.collection.count_documents({})
        print(f"Total de documents : {total_docs}")
        
        # Distribution des genres
        gender_stats = list(self.collection.aggregate([
            {"$group": {"_id": "$patient.Gender", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))
        
        print("Distribution des genres :")
        for stat in gender_stats:
            print(f"  {stat['_id']}: {stat['count']}")
        
        # Statistiques d'âge
        age_stats = list(self.collection.aggregate([
            {
                "$group": {
                    "_id": None,
                    "avg_age": {"$avg": "$patient.Age"},
                    "min_age": {"$min": "$patient.Age"},
                    "max_age": {"$max": "$patient.Age"}
                }
            }
        ]))
        
        if age_stats:
            stats = age_stats[0]
            print(f"Âge moyen : {stats['avg_age']:.1f}")
            print(f"Âge min/max : {stats['min_age']}/{stats['max_age']}")
    
    def run_all_verifications(self):
        """Exécuter toutes les vérifications"""
        print("🚀 DÉBUT DE LA VÉRIFICATION DE MIGRATION")
        print("=" * 50)
        
        if not self.connect_to_mongo():
            return False
        
        # Exécuter toutes les vérifications
        verifications = [
            self.verify_document_count,
            self.verify_data_structure,
            self.verify_data_types,
            self.verify_data_integrity,
            self.verify_duplicates
        ]
        
        success = True
        for verification in verifications:
            try:
                if not verification():
                    success = False
            except Exception as e:
                error_msg = f"❌ Erreur dans {verification.__name__}: {e}"
                print(error_msg)
                self.errors.append(error_msg)
                success = False
        
        # Générer les statistiques
        self.generate_statistics()
        
        # Résumé final
        print("\n" + "=" * 50)
        print("📋 RÉSUMÉ DE LA VÉRIFICATION")
        print("=" * 50)
        
        if self.errors:
            print(f"❌ ERREURS ({len(self.errors)}):")
            for error in self.errors:
                print(f"  • {error}")
        
        if self.warnings:
            print(f"⚠️ AVERTISSEMENTS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        if success and not self.errors:
            print("✅ MIGRATION VÉRIFIÉE AVEC SUCCÈS !")
            return True
        else:
            print("❌ MIGRATION ÉCHOUÉE OU PROBLÉMATIQUE")
            return False
    
    def close_connection(self):
        """Fermer la connexion"""
        if self.client:
            self.client.close()

def main():
    # Configuration
    mongo_uri = os.getenv('MONGO_URI', 'mongodb://user:pwuser@mongodb:27017/healthcare_db?authSource=healthcare_db')
    csv_file = 'medical_data_cleaned.csv'
    
    # Créer et exécuter le vérificateur
    verifier = MigrationVerifier(mongo_uri, csv_file)
    
    try:
        success = verifier.run_all_verifications()
        sys.exit(0 if success else 1)
    finally:
        verifier.close_connection()

if __name__ == "__main__":
    main()
