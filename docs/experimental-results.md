# SmartLogix 360 — Résultats expérimentaux

## 1. Objectif

Cette campagne expérimentale évalue les principales briques de SmartLogix 360 : pipeline Big Data batch, chaîne temps réel Kafka → Spark → Machine Learning → PostgreSQL, modèle de prédiction des retards, simulation du jumeau numérique et optimisation prescriptive.

Les mesures de performance correspondent à l’environnement expérimental local et ne représentent pas une limite théorique de l’architecture distribuée.

## 2. Pipeline Batch

Le pipeline traite les données LaDe de Chongqing, Hangzhou, Jilin, Shanghai et Yantai.

| Indicateur | Valeur |
|---|---:|
| Livraisons traitées | 4 514 661 |
| Durée du DAG Airflow | 1 170,44 s |
| Durée approximative | 19 min 30 s |
| Débit effectif observé | ≈ 3 857 livraisons/s |
| État du run | Success |

Le pipeline couvre l’ingestion, les transformations Bronze/Silver/Gold, la consolidation et le chargement analytique.

## 3. Évaluation du modèle Machine Learning

Le modèle final est LightGBM. Le seuil de classification a été sélectionné sur le jeu de validation puis figé avant l’ouverture du jeu de test final.

| Paramètre | Valeur |
|---|---:|
| Modèle | LightGBM |
| Seuil | 0.246906 |
| Lignes d'entraînement | 105 229 |
| Lignes de test | 50 011 |
| Taux de retard dans le test | 18,68 % |

| Métrique | Valeur |
|---|---:|
| ROC-AUC | 0.832004 |
| PR-AUC | 0.529243 |
| Precision | 0.422115 |
| Recall | 0.727633 |
| F1-score | 0.534282 |
| Balanced Accuracy | 0.749375 |

| Résultat | Nombre |
|---|---:|
| True Negative | 31 359 |
| False Positive | 9 308 |
| False Negative | 2 545 |
| True Positive | 6 799 |

Le rappel de 72,76 % montre que le modèle détecte une part importante des livraisons réellement en retard. Une précision plus faible est cohérente avec le cas d’usage opérationnel, où manquer une livraison à risque peut être plus coûteux qu’une alerte préventive supplémentaire.

| Indicateur | Valeur |
|---|---:|
| Entraînement | 5,036 s |
| Inférence sur 50 011 lignes | 0,766 s |

Les métriques locales doivent être interprétées avec prudence lorsque l’échantillon est faible. Le jeu de test de Jilin ne contient que 13 observations.

## 4. Pipeline temps réel

La chaîne testée est :

**Producteur LaDe → Apache Kafka → Spark Structured Streaming → Enrichissement historique J-1 → LightGBM → PostgreSQL → Digital Twin**

Le benchmark contrôlé utilise 100 événements `delivery_accepted`.

| Indicateur | Valeur |
|---|---:|
| Événements envoyés | 100 |
| Événements traités | 100 |
| Complétude | 100 % |
| Temps global observé | 69,95 s |
| Débit E2E observé | 1,43 événement/s |

Le débit mesuré ne représente pas la limite de SmartLogix 360. Pendant l’expérience, Spark fonctionnait localement avec 1 worker, 1 cœur CPU, environ 1 Go de mémoire executor et un trigger de 5 secondes. Une micro-batch a nécessité environ 54,241 secondes, ce qui montre une saturation de l’environnement local.

## 5. Latence de persistance des prédictions

Cette mesure couvre la création de la prédiction ML jusqu’à sa persistance PostgreSQL, et non toute la chaîne E2E.

| Indicateur | Latence |
|---|---:|
| Moyenne | 62,11 ms |
| P50 | 43,43 ms |
| P95 | 145,88 ms |
| Maximum | 438,23 ms |

## 6. Simulation What-If

L’état utilisé comportait 508 commandes, dont 381 à risque, soit 75 %.

| Scénario | Commandes à risque |
|---|---:|
| Baseline | 381 |
| Demande +20 % | 400 |
| Capacité -20 % | 406 |
| SLA +20 % | 311 |

Les résultats sont des simulations What-If déterministes et non des estimations causales.

## 7. Optimisation prescriptive

Scénario : demande +50 %, capacité testée ×1,00 à ×1,50, budget maximal 0,25 et cible de risque 75 %.

| Indicateur | Valeur |
|---|---:|
| Risque baseline | 381 |
| Risque sous stress | 438 |
| Capacité recommandée | ×1,25 |
| Hausse de capacité | 25 % |
| Risque après intervention | 400 |
| Risques évités vs stress | 38 |
| Coût | 0,25 |
| Risque final | 78,7 % |
| Écart vs baseline | +19 |
| Objectif de 75 % atteint | Non |

Le moteur sélectionne la meilleure intervention faisable sous contraintes, même lorsque l’objectif demandé ne peut pas être entièrement atteint.

## 8. Synthèse

| Composant | Résultat principal |
|---|---|
| Batch | 4 514 661 livraisons en ≈ 19 min 30 s |
| ML | ROC-AUC = 0.8320 ; Recall = 72,76 % |
| Streaming | 100/100 événements traités |
| Streaming | 1,43 événement/s sur environnement local contraint |
| Persistance ML → PostgreSQL | P50 = 43,43 ms ; P95 = 145,88 ms |
| Digital Twin | simulations What-If opérationnelles |
| Optimisation | recommandations sous contraintes de budget et risque |


## 9. Conclusion expérimentale

Les expériences valident le fonctionnement intégré de SmartLogix 360 sur les dimensions batch, streaming, prédictive, simulation et prescriptive.

Le système est capable de traiter plusieurs millions de livraisons historiques, de produire des prédictions à partir des informations disponibles à l’acceptation, de traiter des événements en temps réel, de maintenir un état numérique, de simuler des scénarios What-If et de proposer des interventions sous contraintes.

Les limitations de performance observées sur le streaming sont principalement liées à l’environnement local expérimental contraint. Elles justifient des expérimentations futures sur une infrastructure distribuée avec plusieurs workers Spark et davantage de ressources CPU et mémoire.
