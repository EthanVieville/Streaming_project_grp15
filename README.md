### Postegre

Télécharger et extraire:
https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz

Télécharger :
https://www.enterprisedb.com/downloads/postgres-postgresql-downloads

Version 17.5 W64
Lancer le fichier, tout laisser par défault, sauf le mdp, mettre 1234 (no comment)

Ensuite go variables d'environnement, variables système on rajoute dans path :
"C:\Program Files\PostgreSQL\17\bin"


----

## Outils à lancer : 

#### BD : 

lauch le script run.sh (créer la base) :  
git bash sur la racine du repo : ./run.sh

Launch kafka listener : 

Ouvrir 2 instance git bash sur le racine du dossier kafka_2.13-3.7.0

lancer dans la premiere : bin/zookeeper-server-start.sh config/zookeeper.properties

Lancer dans la deuxième : bin/kafka-server-start.sh config/server.properties


----
Utiliser JAVA11 pour le Projet

Accès à la console SPARK:
http://localhost:4040/jobs/
-----


VIZ : 

(faire les pip install nécessare, conseil très pro)

cd streamlit-app-py

streamlit run app.py


