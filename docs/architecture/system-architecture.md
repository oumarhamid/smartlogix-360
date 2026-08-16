# SmartLogix 360 — Architecture système finale

## 1. Vue générale

SmartLogix 360 est une plateforme Big Data logistique conçue comme un jumeau numérique intelligent. Elle combine traitement batch, temps réel, Machine Learning, supervision opérationnelle, simulation What-If, optimisation prescriptive, API et cockpit web.

Chaîne fonctionnelle :

**Collecter → Traiter → Observer → Prédire → Représenter par Digital Twin → Simuler → Optimiser → Évaluer expérimentalement**

## 2. Architecture Batch

Flux : **LaDe → Airflow → Spark → Bronze → Silver → Gold → Apache Hudi → PostgreSQL → dbt → Metabase**

Bronze conserve les données proches de leur forme source. Silver réalise nettoyage, standardisation et contrôles qualité. Gold produit les jeux de données métier et analytiques.

Principales tables Gold :

- `delivery_fact`
- `courier_daily_performance`
- `city_daily_performance`

Le pipeline est orchestré par `smartlogix_lade_batch_pipeline`.

## 3. Stockage et analytique

Apache Hudi constitue la couche Lakehouse. PostgreSQL assure les accès analytiques applicatifs, le temps réel, les prédictions, les alertes et les métadonnées Airflow. dbt structure la couche métier et exécute les tests de qualité. Metabase fournit la visualisation analytique.

## 4. Architecture temps réel

Le temps de référence est `T0 = accept_timestamp`. Aucune information connue après l’acceptation ne peut être utilisée pour la prédiction.

Flux : **delivery_accepted → Kafka → Spark Structured Streaming → enrichissement T0 + historique J-1 → LightGBM → prédiction → PostgreSQL + alertes → Digital Twin**

## 5. Apache Kafka

Kafka fonctionne en mode KRaft, sans ZooKeeper.

Principaux topics :

- `smartlogix.delivery.events`
- `smartlogix.delivery.accepted`
- `smartlogix.delivery.features`
- `smartlogix.delivery.predictions`
- `smartlogix.delivery.alerts`
- `smartlogix.delivery.dead-letter`

## 6. Spark Structured Streaming

Spark assure la lecture Kafka, la validation, l’enrichissement, le calcul des features temporelles, les jointures avec les historiques J-1 et la publication des features.

Principales familles de features : heure, jour, mois, période de journée, week-end, historique coursier J-1, ville J-1 et AOI J-1.

## 7. Machine Learning

Le modèle final est LightGBM.

**Features disponibles à T0 → LightGBM → probabilité de retard → seuil 0.246906 → classe**

Le seuil a été sélectionné sur validation puis figé avant le test final. L’inférence s’exécute dans un runtime Python dédié pour éviter les incompatibilités avec Spark.

## 8. Persistance temps réel

Tables principales du schéma `realtime` :

- `delivery_events`
- `delivery_events_stage`
- `delivery_live_status`
- `delivery_predictions`
- `delivery_prediction_latest`
- `delivery_alerts`

Les traitements sont idempotents afin que les replays Kafka ne créent pas de doublons incohérents.

## 9. Digital Twin

Le jumeau numérique représente l’état opérationnel courant d’une commande à partir de son identifiant, sa ville, son coursier, son AOI, sa probabilité de retard, son état prédit, la disponibilité des historiques J-1 et l’état d’alerte.

## 10. Simulation What-If

Le moteur évalue les variations de demande, capacité et SLA.

Facteur de pression :

`pressure = demand_multiplier / courier_capacity_multiplier / sla_multiplier`

Les résultats sont des simulations déterministes What-If, non causales.

## 11. Optimisation prescriptive

L’optimisation recherche une intervention selon une plage de capacité, un pas, un budget, un coût unitaire et une cible de risque. Elle compare les candidats et sélectionne la meilleure solution faisable, tout en pouvant indiquer que l’objectif est inaccessible.

## 12. API Digital Twin

FastAPI expose :

- `GET /health`
- `GET /api/v1/twin/state`
- `POST /api/v1/twin/simulations`
- `POST /api/v1/twin/experiments`
- `POST /api/v1/twin/optimizations`

L’API appelle directement les modules Python du Digital Twin.

## 13. Cockpit React

Le cockpit React comporte trois vues :

- vue opérationnelle ;
- simulation ;
- optimisation.

Il affiche les KPI, les états par ville, les probabilités de retard, les résultats de simulation et les recommandations prescriptives.

## 14. Conteneurisation

Principaux composants Dockerisés :

- PostgreSQL
- Kafka
- Spark Master
- Spark Worker
- Spark Structured Streaming
- ML Inference
- Airflow
- Metabase
- Digital Twin API

Le frontend React utilise Vite et TypeScript.

## 15. Principes architecturaux

- absence de fuite temporelle ;
- historiques strictement J-1 ;
- idempotence ;
- séparation claire des responsabilités ;
- architecture indépendante des limites du poste local.

Répartition des responsabilités :

- Airflow → orchestration batch
- Spark → traitement distribué
- Kafka → transport événementiel
- LightGBM → prédiction
- PostgreSQL → persistance
- Digital Twin → représentation opérationnelle
- FastAPI → services
- React → interaction utilisateur

## 16. Synthèse

SmartLogix 360 dépasse une plateforme analytique classique pour devenir un jumeau numérique logistique intelligent combinant Big Data, temps réel, Machine Learning, simulation et optimisation.
