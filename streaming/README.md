# SmartLogix 360 - Streaming temps reel

Cette couche complete le pipeline batch LaDe avec un flux temps reel base sur Kafka,
Spark Structured Streaming et PostgreSQL.

## Flux valide

```text
Gold LaDe
   -> Producer Python
   -> Kafka
   -> Consumer de validation
   -> Spark Structured Streaming
   -> PostgreSQL realtime
```

## Topics

- `smartlogix.delivery.events`
- `smartlogix.delivery.alerts`
- `smartlogix.delivery.dead-letter`

## PostgreSQL temps reel

Le schema `realtime` est separe des tables analytiques batch.

- `realtime.delivery_events` : historique append-only des evenements Kafka.
- `realtime.delivery_live_status` : dernier etat connu de chaque `order_id`.
- `realtime.delivery_events_stage` : staging UNLOGGED utilise par micro-batch Spark.

Le sink Structured Streaming est idempotent :

- `event_id` empeche la duplication d'un evenement rejoue ;
- le statut live n'est remplace que par un evenement plus recent ;
- le staging est fusionne et nettoye dans une transaction PostgreSQL ;
- le checkpoint Spark est persiste sous `data/checkpoints/streaming/`.

## Execution locale

Kafka et Spark sont regroupes dans le profil Docker `streaming`. Les ressources locales
restent configurables dans `.env` et peuvent etre augmentees sur une machine plus
puissante sans modifier le code metier.

Le cache Ivy Spark est persiste dans un volume Docker pour eviter de telecharger a
nouveau les dependances Kafka a chaque recreation des conteneurs.

## Suite

Apres validation Kafka -> Spark -> PostgreSQL, la prochaine couche ajoutera :

1. entrainement et comparaison des modeles de prediction de retard ;
2. inference dans le flux Spark ;
3. `realtime.delivery_predictions` ;
4. `realtime.delivery_alerts` ;
5. alimentation du dashboard et du jumeau numerique.
