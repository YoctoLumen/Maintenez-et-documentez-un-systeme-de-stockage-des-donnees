// init_mongo.js - Version corrigée
print('=== DÉBUT INITIALISATION MONGODB AVEC AUTHENTIFICATION ===');

try {
    // === CRÉATION DE L'UTILISATEUR POUR HEALTHCARE_DB ===
    db = db.getSiblingDB("healthcare_db");
    
    print('Création de l\'utilisateur applicatif...');
    db.createUser({
        user: "user",
        pwd: "pwuser",
        roles: [
            { role: "readWrite", db: "healthcare_db" }
        ]
    });
    print('✅ Utilisateur "user" créé pour healthcare_db');

    // Créer la collection Patients
    db.createCollection('Patients');
    print('✅ Collection Patients créée');

    // === CRÉATION DE L'ADMINISTRATEUR ===
    db = db.getSiblingDB("admin");
    
    print('Création de l\'administrateur principal...');
    db.createUser({
        user: "admin",
        pwd: "pwadmin",
        roles: [
            { role: "userAdminAnyDatabase", db: "admin" },
            { role: "readWriteAnyDatabase", db: "admin" },
            { role: "dbAdminAnyDatabase", db: "admin" }
        ]
    });
    print('✅ Administrateur principal créé');

    print('=== RÉSUMÉ DES UTILISATEURS CRÉÉS ===');
    print('1. user (healthcare_db) : Utilisateur applicatif - Lecture/Écriture');
    print('2. admin (admin) : Administrateur principal - Accès total');

} catch (error) {
    print('❌ ERREUR lors de l\'initialisation:', error.message);
    print('Code d\'erreur:', error.code);
}

print('=== FIN INITIALISATION MONGODB ===');
