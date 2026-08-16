# SmartLogix 360 — Décisions techniques essentielles

## 1. Prédiction au temps T0

Le moment de référence de la prédiction est `T0 = accept_timestamp`.

Aucune information postérieure à l’acceptation de la livraison n’est utilisée pour produire une prédiction.

Cette décision empêche toute fuite temporelle entre les données d’entraînement et les informations réellement disponibles au moment de l’inférence.

## 2. Historiques strictement J-1

Les variables historiques utilisées par le modèle sont calculées exclusivement à partir des données disponibles au jour précédent.

Les historiques concernent notamment :

- le coursier ;
- la ville ;
- l’AOI.

Cette approche garantit qu’aucune information future n’est utilisée lors d’une prédiction temps réel.

## 3. Modèle Machine Learning final

Le modèle final retenu est **LightGBM**.

Le seuil de classification final est `0.246906`.

Le modèle et le seuil ont été sélectionnés à partir du jeu de validation.

Le jeu de test final n’a été utilisé qu’après fixation définitive du modèle et du seuil afin d’éviter tout ajustement sur les données de test.

## 4. Séparation Spark et runtime Machine Learning

Spark Structured Streaming et le moteur d’inférence Machine Learning utilisent des environnements Python distincts.

Cette séparation permet :

- de préserver la stabilité de l’environnement Spark ;
- d’utiliser les versions de bibliothèques nécessaires au modèle final ;
- d’éviter les incompatibilités entre Spark et les dépendances Machine Learning.

Le modèle final est donc exécuté dans un runtime d’inférence dédié.

## 5. Kafka en mode KRaft

Apache Kafka fonctionne en mode **KRaft**, sans ZooKeeper.

Ce choix réduit le nombre de composants nécessaires et correspond à l’architecture moderne de Kafka.

Plusieurs topics spécialisés sont utilisés pour séparer :

- les événements logistiques ;
- les événements d’acceptation ;
- les features ;
- les prédictions ;
- les alertes ;
- les événements en erreur.

## 6. Idempotence des traitements temps réel

Les événements et les prédictions utilisent des identifiants permettant de reconnaître les données déjà traitées.

Lors d’un replay Kafka, une prédiction déjà persistée ne doit donc pas créer un doublon incohérent dans PostgreSQL.

Cette propriété facilite les replays, les tests et la reprise après incident.

## 7. PostgreSQL comme couche opérationnelle

PostgreSQL assure la persistance opérationnelle de plusieurs catégories de données :

- événements temps réel ;
- états courants ;
- prédictions ;
- alertes ;
- résultats nécessaires au Digital Twin.

Le Lakehouse reste destiné au stockage et aux traitements analytiques massifs.

PostgreSQL fournit quant à lui une couche adaptée aux consultations rapides des services applicatifs.

## 8. FastAPI pour l’API du Digital Twin

FastAPI expose directement les modules Python existants du jumeau numérique.

Cette architecture évite de réimplémenter dans un autre langage :

- la représentation du Digital Twin ;
- la logique de simulation ;
- le moteur d’expérimentation ;
- l’optimisation prescriptive.

L’ajout de Spring Boot n’apporte donc pas de valeur suffisante dans l’architecture finale actuelle.

## 9. Digital Twin comme couche centrale

Le Digital Twin ne constitue pas uniquement une interface de visualisation.

Il représente l’état opérationnel courant à partir :

- des événements ;
- des prédictions ;
- des historiques ;
- des alertes.

Il supporte la chaîne fonctionnelle suivante :

**Observer → Prédire → Simuler → Optimiser**

Le Digital Twin constitue ainsi le point de convergence entre les données temps réel, le Machine Learning et les fonctions prescriptives.

## 10. Simulation What-If

Le moteur de simulation permet d’étudier différents scénarios opérationnels, notamment :

- augmentation de la demande ;
- diminution de la capacité ;
- augmentation de la capacité ;
- variation du SLA ;
- combinaison de plusieurs contraintes.

Le moteur repose sur un modèle déterministe de pression opérationnelle.

Les résultats doivent donc être présentés comme des simulations **What-If** et non comme des estimations causales.

Cette distinction doit être conservée dans le rapport et lors de la soutenance.

## 11. Optimisation prescriptive sous contraintes

Le moteur prescriptif recherche la meilleure intervention possible en tenant compte :

- du budget disponible ;
- du coût unitaire ;
- de la capacité minimale ;
- de la capacité maximale ;
- du pas de recherche ;
- du niveau de risque cible.

Le moteur sélectionne la meilleure solution faisable.

Il peut également indiquer explicitement qu’un objectif de risque ne peut pas être atteint avec les ressources ou le budget disponibles.

## 12. Architecture scalable indépendante du poste local

Le poste local est utilisé uniquement pour :

- le développement ;
- les tests fonctionnels ;
- les démonstrations ;
- les expériences sur des charges contrôlées.

Les performances mesurées localement ne représentent pas les limites théoriques de SmartLogix 360.

La plateforme est conçue pour évoluer vers une infrastructure distribuée ou cloud comprenant notamment :

- plusieurs workers Spark ;
- davantage de CPU ;
- davantage de mémoire ;
- des services distribués ;
- des tests de charge exécutés sur des ressources adaptées.

Cette séparation permet de conserver une architecture professionnelle et scalable sans la limiter aux ressources du poste de développement.