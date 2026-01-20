import subprocess
import sys
import os

def run_migration_tests():
    
    print("🧪 LANCEMENT DES TESTS DE MIGRATION")
    print("=" * 50)
    
    # 1. Test de connexion
    print("1. Test de connexion à MongoDB...")
    try:
        from pymongo import MongoClient
        mongo_uri = os.getenv('MONGO_URI', 'mongodb://user:pwuser@mongodb:27017/healthcare_db?authSource=healthcare_db')
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        client.close()
        print("✅ Connexion MongoDB OK")
    except Exception as e:
        print(f"❌ Connexion MongoDB échouée : {e}")
        return False
    
    # 2. Test de vérification de migration
    print("\n2. Vérification de la migration...")
    try:
        result = subprocess.run([
            'python', 'verify_migration.py'
        ], capture_output=True, text=True, timeout=300)
        
        print(result.stdout)
        if result.stderr:
            print("STDERR:", result.stderr)
        
        if result.returncode == 0:
            print("✅ Vérification de migration réussie")
            return True
        else:
            print("❌ Vérification de migration échouée")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Timeout lors de la vérification")
        return False
    except Exception as e:
        print(f"❌ Erreur lors de la vérification : {e}")
        return False

if __name__ == "__main__":
    success = run_migration_tests()
    sys.exit(0 if success else 1)
