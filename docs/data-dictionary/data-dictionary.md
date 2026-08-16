# SmartLogix 360 — Dictionnaire de données

## 1. Tables analytiques Gold

### delivery_fact

Table principale des livraisons consolidées.

| Champ | Description |
|---|---|
| order_id | Identifiant unique de la livraison |
| city | Ville |
| courier_id | Identifiant du coursier |
| aoi_id | Identifiant de la zone AOI |
| accept_timestamp | Date et heure d'acceptation |
| delivery_duration_minutes | Durée de livraison |
| sla_minutes | SLA associé |
| is_within_sla | Respect du SLA |
| is_late_delivery | Livraison considérée en retard |
| is_quality_warning | Indicateur qualité |

Volume consolidé : **4 514 661 livraisons**.

### courier_daily_performance

Agrégats journaliers par coursier : commandes, retards, taux de retard, durée moyenne, SLA et qualité.

### city_daily_performance

Agrégats journaliers par ville : volume, taux de retard, durée moyenne, SLA et qualité opérationnelle.

## 2. Tables temps réel

Schéma PostgreSQL : `realtime`.

### delivery_events

| Champ | Description |
|---|---|
| event_id | Identifiant unique de l'événement |
| event_type | Type d'événement |
| event_time | Horodatage événementiel |
| source_event_time | Horodatage métier d'origine |
| order_id | Identifiant commande |
| city | Ville |
| courier_id | Coursier |
| aoi_id | AOI |
| kafka_partition | Partition Kafka |
| kafka_offset | Offset Kafka |
| kafka_timestamp | Horodatage Kafka |
| ingested_at | Date de persistance |

### delivery_events_stage

Zone intermédiaire de traitement temps réel.

### delivery_live_status

Dernier état opérationnel connu d'une livraison.

## 3. Prédictions

### delivery_predictions

| Champ | Description |
|---|---|
| prediction_id | Identifiant unique de la prédiction |
| source_event_id | Événement déclencheur |
| event_time | Date de création de la prédiction |
| source_event_time | Date métier d'acceptation |
| order_id | Commande concernée |
| city | Ville |
| courier_id | Coursier |
| delay_probability | Probabilité estimée de retard |
| predicted_late | Classe prédite |
| model_name | Nom du modèle |
| model_version | Version du modèle |
| courier_prev_day_available | Historique J-1 coursier disponible |
| ingested_at | Date de persistance PostgreSQL |

### delivery_prediction_latest

Dernière prédiction connue pour chaque commande, utilisée par le Digital Twin pour récupérer rapidement l’état prédictif courant.

## 4. Alertes

### delivery_alerts

Stocke les alertes lorsque le risque dépasse le seuil final `0.246906`.

## 5. Features Machine Learning

Les features sont construites à partir des informations disponibles à `T0 = accept_timestamp`.

### Features temporelles

- `accept_hour`
- `accept_weekday`
- `accept_month`
- `accept_day`
- `accept_period`
- `accept_is_weekend`

### Historique coursier J-1

- `courier_prev_day_orders_total`
- `courier_prev_day_orders_late`
- `courier_prev_day_avg_duration`
- `courier_prev_day_sla_rate`
- `courier_prev_day_warning_rate`
- `courier_prev_day_available`

### Historique ville J-1

- `city_prev_day_orders_total`
- `city_prev_day_orders_late`
- `city_prev_day_avg_duration`
- `city_prev_day_sla_rate`
- `city_prev_day_warning_rate`
- `city_prev_day_available`

### Historique AOI J-1

- `aoi_prev_day_orders_total`
- `aoi_prev_day_orders_late`
- `aoi_prev_day_avg_duration`
- `aoi_prev_day_sla_rate`
- `aoi_prev_day_warning_rate`
- `aoi_prev_day_available`

Aucune information postérieure à T0 ne peut être utilisée pour prédire le retard.

## 6. État du Digital Twin

| Champ | Description |
|---|---|
| order_id | Commande |
| source_event_time | Horodatage métier |
| city | Ville |
| courier_id | Coursier |
| aoi_id | Zone AOI |
| delay_probability | Probabilité de retard |
| predicted_late | État prédictif |
| threshold | Seuil utilisé |
| model_name | Modèle |
| model_version | Version |
| courier_prev_day_available | Historique coursier disponible |
| city_prev_day_available | Historique ville disponible |
| alert_active | Alerte active |
| updated_at | Dernière mise à jour |

## 7. Principes de qualité

- unicité des identifiants ;
- contrôle des valeurs nulles critiques ;
- cohérence des timestamps ;
- séparation batch / temps réel ;
- historique strict J-1 pour le ML ;
- idempotence des prédictions et événements rejoués ;
- traçabilité Kafka via partition et offset.
