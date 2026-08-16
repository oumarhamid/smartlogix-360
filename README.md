# SmartLogix 360

## Jumeau numérique Big Data temps réel pour la supervision, la prédiction et l’optimisation des opérations logistiques

SmartLogix 360 est une plateforme logistique intelligente combinant **Big Data, traitement temps réel, Machine Learning, Digital Twin, simulation What-If et optimisation prescriptive**.

L’objectif du projet est de dépasser une plateforme analytique classique afin de construire un système capable de :

- collecter et transformer des données logistiques massives ;
- superviser les opérations historiques et temps réel ;
- prédire les risques de retard dès l’acceptation d’une livraison ;
- représenter l’état opérationnel dans un jumeau numérique ;
- simuler des scénarios logistiques ;
- recommander des interventions sous contraintes de budget, capacité et risque.

---

## Sommaire

- [Vision du projet](#vision-du-projet)
- [Architecture générale](#architecture-générale)
- [Pipeline Batch](#pipeline-batch)
- [Pipeline temps réel](#pipeline-temps-réel)
- [Machine Learning](#machine-learning)
- [Digital Twin](#digital-twin)
- [Simulation What-If](#simulation-what-if)
- [Optimisation prescriptive](#optimisation-prescriptive)
- [Cockpit opérationnel](#cockpit-opérationnel)
- [Résultats expérimentaux](#résultats-expérimentaux)
- [Stack technique](#stack-technique)
- [Structure du dépôt](#structure-du-dépôt)
- [Lancement du projet](#lancement-du-projet)
- [API Digital Twin](#api-digital-twin)
- [Tests et qualité](#tests-et-qualité)
- [Documentation](#documentation)
- [Limites et perspectives](#limites-et-perspectives)

---

## Vision du projet

La chaîne fonctionnelle de SmartLogix 360 est :

**Collecter → Traiter → Observer → Prédire → Représenter → Simuler → Optimiser → Évaluer**

Le projet suit une architecture hybride composée de deux flux complémentaires :

- un pipeline **Batch** pour exploiter les données historiques ;
- un pipeline **Realtime** pour traiter les événements d’acceptation des livraisons.

Ces deux flux convergent vers le **Digital Twin logistique**.

---

## Architecture générale

```text
                         DONNÉES LaDe
                              │
                  ┌───────────┴───────────┐
                  │                       │
                BATCH                  REALTIME
                  │                       │
               Airflow                  Kafka
                  │                       │
                Spark           Spark Structured Streaming
                  │                       │
        Bronze / Silver / Gold      Features T0 + J-1
                  │                       │
                Hudi                  LightGBM
                  │                       │
            PostgreSQL              Prédictions
                  │                       │
            dbt / Metabase             Alertes
                                          │
                                    DIGITAL TWIN
                                          │
                           ┌──────────────┴──────────────┐
                           │                             │
                       Simulation                   Optimisation
                           │                             │
                           └──────────────┬──────────────┘
                                          │
                                       FastAPI
                                          │
                                        React
```

---

## Pipeline Batch

Le pipeline Batch transforme les données historiques LaDe en datasets analytiques fiables.

```text
LaDe
  ↓
Airflow
  ↓
Spark
  ↓
Bronze
  ↓
Silver
  ↓
Gold
  ↓
Apache Hudi
  ↓
PostgreSQL
  ↓
dbt
  ↓
Metabase
```

### Couches de données

**Bronze**

Conservation des données proches de leur format source afin de garantir la traçabilité.

**Silver**

Nettoyage, standardisation, contrôle qualité et préparation des données.

**Gold**

Production des datasets orientés métier et analytique.

Principales tables Gold :

- `delivery_fact`
- `courier_daily_performance`
- `city_daily_performance`

Le pipeline est orchestré par le DAG Airflow :

`smartlogix_lade_batch_pipeline`

---

## Pipeline temps réel

La chaîne temps réel débute dès l’acceptation d’une livraison.

```text
delivery_accepted
        ↓
      Kafka
        ↓
Spark Structured Streaming
        ↓
Features temporelles T0
+
Historiques strictement J-1
        ↓
     LightGBM
        ↓
   Prédiction
        ↓
PostgreSQL + Alertes
        ↓
   Digital Twin
```

Kafka fonctionne en mode **KRaft**, sans ZooKeeper.

### Topics principaux

- `smartlogix.delivery.events`
- `smartlogix.delivery.accepted`
- `smartlogix.delivery.features`
- `smartlogix.delivery.predictions`
- `smartlogix.delivery.alerts`
- `smartlogix.delivery.dead-letter`

---

## Machine Learning

SmartLogix 360 prédit le risque de retard dès le moment de l’acceptation de la livraison.

Le moment de référence est :

`T0 = accept_timestamp`

Aucune information postérieure à T0 n’est utilisée pour produire une prédiction.

Les historiques sont strictement calculés à **J-1** afin d’éviter toute fuite temporelle.

### Features principales

Features temporelles :

- heure d’acceptation ;
- jour de la semaine ;
- mois ;
- période de la journée ;
- week-end.

Historiques J-1 :

- coursier ;
- ville ;
- AOI.

### Modèle final

- Modèle : **LightGBM**
- Seuil de classification : **0.246906**
- Version : `lightgbm-delay-v1`

Le modèle et le seuil ont été sélectionnés sur le jeu de validation avant l’ouverture du jeu de test final.

### Résultats du test final

| Métrique | Valeur |
|---|---:|
| Lignes de test | 50 011 |
| ROC-AUC | 0.832004 |
| PR-AUC | 0.529243 |
| Precision | 0.422115 |
| Recall | 0.727633 |
| F1-score | 0.534282 |
| Balanced Accuracy | 0.749375 |

### Matrice de confusion

| Classe | Nombre |
|---|---:|
| True Negative | 31 359 |
| False Positive | 9 308 |
| False Negative | 2 545 |
| True Positive | 6 799 |

Le rappel de **72,76 %** permet de détecter une part importante des livraisons réellement en retard.

---

## Digital Twin

Le Digital Twin représente l’état opérationnel courant des opérations logistiques.

Pour chaque livraison, il peut notamment conserver :

- commande ;
- ville ;
- coursier ;
- AOI ;
- probabilité de retard ;
- prédiction ;
- historique J-1 disponible ;
- alerte active ;
- dernière mise à jour.

Exemple d’état expérimental :

| Indicateur | Valeur |
|---|---:|
| Commandes représentées | 508 |
| Commandes à risque | 381 |
| Taux de risque initial | 75 % |

Le Digital Twin constitue le point de convergence entre :

**Observation → Prédiction → Simulation → Optimisation**

---

## Simulation What-If

Le moteur de simulation permet d’explorer des scénarios opérationnels sans modifier le système réel.

Exemples :

| Scénario | Commandes à risque |
|---|---:|
| Baseline | 381 |
| Demande +20 % | 400 |
| Capacité -20 % | 406 |
| SLA +20 % | 311 |

Le modèle utilise un facteur de pression opérationnelle :

`pressure = demand_multiplier / courier_capacity_multiplier / sla_multiplier`

Les résultats doivent être interprétés comme des simulations **What-If déterministes** et non comme des estimations causales.

---

## Optimisation prescriptive

Le moteur d’optimisation recherche la meilleure intervention réalisable selon plusieurs contraintes :

- demande ;
- capacité minimale ;
- capacité maximale ;
- pas de recherche ;
- budget ;
- coût unitaire ;
- niveau de risque cible.

### Exemple

Scénario :

- demande : +50 % ;
- capacité : ×1.00 à ×1.50 ;
- budget maximal : 0.25 ;
- objectif de risque : 75 %.

Résultat :

| Indicateur | Valeur |
|---|---:|
| Risque baseline | 381 |
| Risque sous stress | 438 |
| Capacité recommandée | ×1.25 |
| Hausse de capacité | 25 % |
| Risque après intervention | 400 |
| Risques évités | 38 |
| Coût | 0.25 |
| Risque final | 78.7 % |
| Objectif atteint | Non |

Le moteur peut donc identifier la meilleure solution faisable même lorsqu’un objectif demandé ne peut pas être complètement atteint.

---

## Cockpit opérationnel

Le cockpit SmartLogix 360 est développé avec **React + TypeScript + Vite** et consomme l’API FastAPI.

Il comporte trois vues principales.

### Vue opérationnelle

Affiche notamment :

- nombre de commandes ;
- commandes à risque ;
- couverture historique J-1 ;
- état par ville ;
- probabilités de retard ;
- état de l’API.

### Simulation

Permet de modifier :

- demande ;
- capacité ;
- SLA ;
- intensité du stress ;
- ville cible.

### Optimisation

Permet de définir :

- demande ;
- plage de capacité ;
- budget ;
- coût unitaire ;
- risque cible ;
- ville cible.

---

## Résultats expérimentaux

### Batch

| Indicateur | Valeur |
|---|---:|
| Livraisons traitées | 4 514 661 |
| Durée observée | 1 170.44 s |
| Durée approximative | 19 min 30 s |
| Débit observé | ≈ 3 857 livraisons/s |

### Streaming contrôlé

| Indicateur | Valeur |
|---|---:|
| Événements envoyés | 100 |
| Événements traités | 100 |
| Complétude | 100 % |
| Temps observé | 69.95 s |
| Débit E2E local | 1.43 événement/s |

Le benchmark streaming a été exécuté sur un environnement local fortement contraint avec un worker Spark disposant d’environ un cœur CPU et 1 Go de mémoire.

Il ne représente donc pas la limite théorique de l’architecture distribuée.

### Persistance ML → PostgreSQL

| Indicateur | Valeur |
|---|---:|
| Moyenne | 62.11 ms |
| P50 | 43.43 ms |
| P95 | 145.88 ms |
| Maximum | 438.23 ms |

---

## Stack technique

### Data Engineering

- Apache Airflow
- Apache Spark
- Apache Hudi
- dbt
- MinIO
- PostgreSQL

### Streaming

- Apache Kafka
- Spark Structured Streaming

### Machine Learning

- Python
- pandas
- scikit-learn
- LightGBM

### Digital Twin

- Python
- PostgreSQL
- FastAPI

### Frontend

- React
- TypeScript
- Vite

### Visualisation

- Metabase
- Cockpit React

### Infrastructure

- Docker
- Docker Compose

### Qualité

- pytest
- Ruff
- Oxlint

---

## Structure du dépôt

```text
smartlogix-360/
│
├── airflow/
│   └── dags/
│
├── artifacts/
│   └── ml/
│
├── data/
│
├── dbt/
│   └── smartlogix/
│
├── docs/
│   ├── architecture/
│   ├── data-dictionary/
│   ├── decisions/
│   └── experimental-results.md
│
├── infrastructure/
│   └── digital-twin-api/
│
├── react-dashboard/
│
├── scripts/
│
├── src/
│   └── smartlogix/
│       ├── api/
│       ├── digital_twin/
│       └── ml/
│
├── streaming/
│   ├── inference/
│   ├── producers/
│   └── spark/
│
├── tests/
│
├── docker-compose.yml
└── README.md
```

---

## Lancement du projet

### Prérequis

- Docker / Docker Compose
- Python 3.11
- Node.js
- Git

### Cloner le dépôt

```bash
git clone https://github.com/oumarhamid/smartlogix-360.git
cd smartlogix-360
```

### Environnement Python

Sous Windows :

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Configuration

Les paramètres sensibles doivent être placés dans un fichier `.env` local.

Les secrets, mots de passe et tokens ne doivent jamais être versionnés dans Git.

### Services Docker

Les services peuvent être lancés via Docker Compose selon les composants nécessaires à l’expérience ou à la démonstration.

Exemple :

```bash
docker compose up -d
```

### Frontend React

```bash
cd react-dashboard
npm install
npm run dev
```

Le serveur Vite affiche l’URL locale utilisée pour accéder au cockpit.

---

## API Digital Twin

L’API FastAPI expose les principales opérations du jumeau numérique.

### Healthcheck

`GET /health`

### État du Digital Twin

`GET /api/v1/twin/state`

### Simulation

`POST /api/v1/twin/simulations`

### Expériences

`POST /api/v1/twin/experiments`

### Optimisation

`POST /api/v1/twin/optimizations`

---

## Tests et qualité

Le projet comprend des tests unitaires couvrant notamment :

- Machine Learning ;
- Digital Twin ;
- simulation ;
- optimisation ;
- API.

Exécution :

```bash
python -m pytest -q
```

Analyse statique Python :

```bash
ruff check .
```

Frontend :

```bash
cd react-dashboard
npm run lint
npm run build
```

---

## Documentation

La documentation technique est disponible dans `docs/`.

### Architecture

`docs/architecture/system-architecture.md`

### Dictionnaire de données

`docs/data-dictionary/data-dictionary.md`

### Décisions techniques

`docs/decisions/technical-decisions.md`

### Résultats expérimentaux

`docs/experimental-results.md`

---

## Principes d’architecture

### Absence de fuite temporelle

Aucune donnée postérieure à `T0` n’est utilisée pour prédire le retard.

### Historique strict J-1

Les historiques utilisés par le ML sont construits uniquement à partir des données du passé.

### Idempotence

Les replays Kafka ne doivent pas créer de doublons incohérents.

### Séparation des responsabilités

- Airflow → orchestration
- Spark → traitement distribué
- Kafka → événements
- LightGBM → prédiction
- PostgreSQL → persistance opérationnelle
- Digital Twin → représentation
- FastAPI → services
- React → interaction utilisateur

---

## Limites et perspectives

Les performances du pipeline temps réel ont été mesurées sur un environnement local de développement volontairement limité.

L’architecture SmartLogix 360 n’est pas dimensionnée selon les ressources du poste local.

Les principales perspectives sont :

- déploiement sur infrastructure cloud ;
- augmentation du nombre de workers Spark ;
- tests de charge distribués ;
- autoscaling ;
- simulation logistique plus avancée ;
- optimisation multi-objectifs ;
- intégration de nouvelles sources temps réel ;
- observabilité avancée ;
- amélioration continue des modèles prédictifs.

---

## Conclusion

SmartLogix 360 illustre l’évolution d’une plateforme Data moderne vers un système logistique intelligent :

**Data Platform → Realtime Platform → Predictive System → Digital Twin → Prescriptive System**

La plateforme ne se limite donc pas à analyser les opérations passées : elle permet également d’anticiper les risques, de simuler l’impact de changements opérationnels et de proposer des actions sous contraintes.