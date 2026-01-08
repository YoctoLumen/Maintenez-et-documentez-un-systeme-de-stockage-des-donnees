# Projet 5 Openclassroom - Maintenez-et-documentez-un-systeme-de-stockage-des-donnees
Projets Openclassroom numéro 5, dans le cadre de la formation data-engineer

# Description 
L'objectif du projet est la migration d'un ensemble de données médicales vers une base NOSQL. 
Plusieurs contraintes doivent être respecter : 
  - L'ensemble doit être "portable" et "scalable"
  - La migration doit être réalisé de manière conteneuriser au moyen d'un script

# Architecture
  - MongoDB : Base de données NoSQL pour stocker les données patients
  - Migrator : Service de migration des données CSV vers MongoDB
  - Tester : Service de vérification et validation des données migrées
  - Docker Compose : Orchestration des services

# Structure
    projet-healthcare/
    ├── docker-compose.yml          # Configuration des services
    ├── Dockerfile                  # Image pour migrator et tester
    ├── requirements.txt            # Dépendances Python
    ├── init_mongo.js              # Script d'initialisation MongoDB
    ├── data.csv                   # Fichier de données à migrer
    └── app/                       # Scripts Python
        ├── migrer_data.py         # Script de migration principal
        ├── verify_migration.py    # Script de vérification
        ├── cleaning.py            # Fonctions de nettoyage des données
        ├── check_data.py          # Vérifications supplémentaires
        └── verif_data.py          # Validations des données
# Prérequis
  - Docker (version 20.10+)
  - Docker Compose (version 2.0+)
  - Git (optionnel, pour cloner le projet)

## Vérification des prérequis :
        docker --version
        docker-compose --version
# Installation et vérification 
  ## 1- Cloner le projet
    git clone <url-du-projet>
    cd projet-healthcare
  ## 2- Vérification 
    ls -la
    # devrait afficher : docker-compose.yml, Dockerfile, data.csv, app/
# Utilisation
## Exécution 
    docker-compose up --build
## Suivis 
    docker-compose logs -f            # affiche les logs de tous les services
    docker-compose logs -f migrator   # affiche les logs de migration
    docker-compose logs -f tester     # affiche les logs de test
    docker-compose ps                 # affiche l'état du service

# Exécution étape par étape au besoin
## Migration uniquement
    docker-compose up --build migrator
    docker-compose exec mongodb mongosh -u user -p pwuser --authenticationDatabase healthcare_db healthcare_db --eval "db.Patients.countDocuments()"
## Test après migration 
    docker-compose up tester
## Accès direct a mongoDb 
    docker-compose exec mongodb mongosh -u user -p pwuser --authentificationDatabase healthcare_db healthcare_db
# Nettoyage
    docker-compose down            # supprime les containers
    docker-compose down -v         # supprime les volumes ( données mongodb )
    docker-compose down -rmi all   # supprime les images
    docker system prune -a         # nettoyage total ( risqué )

    
