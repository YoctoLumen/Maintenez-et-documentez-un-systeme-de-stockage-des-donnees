import os
from pymongo import MongoClient


# Fonction de test permettant la vérification des rôles dans la base de donnée
# Les uri de connexion sont Hardcoded et peuvent être modifiés
# Ce code est prévu pour être éxécuté dans le cadre du projet 5 de la formation Data Engineer Openclassroom 
# Il doit être lancé sur une base mongodb 

def test_user_permissions():

    print("=== TEST UTILISATEUR APPLICATIF ===")
    try:
        app_uri = "mongodb://user:pwuser@mongodb:27017/healthcare_db?authSource=healthcare_db"
        app_client = MongoClient(app_uri)
        app_db = app_client['healthcare_db']
        
        # Test lecture/écriture
        result = app_db.test.insert_one({"test": "user"})
        print("✅ Écriture réussie pour user")
        
        doc = app_db.test.find_one({"_id": result.inserted_id})
        print("✅ Lecture réussie pour user")
        
        app_db.test.delete_one({"_id": result.inserted_id})
        print("✅ Suppression réussie pour user")
        
    except Exception as e:
        print(f"❌ Erreur user : {e}")
    
    # Test administrateur healthcare
    print("\n=== TEST ADMINISTRATEUR HEALTHCARE ===")
    try:
        admin_uri = "mongodb://admin:pwadmin@mongodb:27017/healthcare_db?authSource=healthcare_db"
        admin_client = MongoClient(admin_uri)
        admin_db = admin_client['healthcare_db']
        
        # Test création d'index (privilège admin)
        admin_db.test_admin.create_index("test_field")
        print("✅ Création d'index réussie pour admin")
        
        # Test lecture/écriture
        result = admin_db.test_admin.insert_one({"test": "admin"})
        print("✅ Écriture réussie pour admin")
        
        # Nettoyage
        admin_db.test_admin.drop()
        print("✅ Suppression de collection réussie pour admin")
        
    except Exception as e:
        print(f"❌ Erreur admin : {e}")

if __name__ == "__main__":
    test_user_permissions()
