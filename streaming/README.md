# SmartLogix 360 - Streaming temps reel

Cette couche complete le pipeline batch LaDe avec un flux temps reel base sur Kafka et
Spark Structured Streaming.

## Flux initial

```text
Gold LaDe -> Producer Python -> Kafka -> Consumer de validation
                                  |
                                  +-> Spark Structured Streaming
```

## Topics

- `smartlogix.delivery.events`
- `smartlogix.delivery.alerts`
- `smartlogix.delivery.dead-letter`

## Principes

- Le producer lit les fichiers Parquet Gold par lots avec PyArrow.
- Les cinq villes sont entrelacees en round-robin.
- Kafka utilise un broker KRaft local.
- Spark utilise un Master et un Worker en mode standalone local.
- Les ressources locales sont configurables par variables d'environnement.
- Les etapes PostgreSQL temps reel, prediction ML et alertes seront ajoutees apres
  validation de Kafka -> Spark.
