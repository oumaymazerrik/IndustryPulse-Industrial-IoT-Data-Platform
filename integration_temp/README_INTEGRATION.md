# IndustryPulse — intégration Streaming

## Prérequis
- Conserver le `docker-compose.yml` existant, le DAG Batch et `spark-runner/server.py`.
- Ne pas remplacer les scripts Batch.
- Prévoir suffisamment de RAM Docker pour 4 JVM Spark (runner + 3 drivers).
- Airflow doit avoir `psycopg2` disponible dans ses conteneurs d’exécution.

## Installation
1. Copier les fichiers `src/`, `spark/` et `airflow/dags/` du paquet vers les mêmes dossiers du projet. Sauvegarder les versions précédentes.
2. Copier `docker-compose.streaming.yml` à la racine du projet.
3. Supprimer de `airflow/dags/` les deux anciens fichiers `industrypulse_gold_sync.py` et `industrypulse_streaming_monitor.py` **après sauvegarde**.
4. Vérifier `docker compose -f docker-compose.yml -f docker-compose.streaming.yml config --quiet`.
5. Lancer `docker compose -f docker-compose.yml -f docker-compose.streaming.yml up -d --build`.
6. Vérifier `docker compose -f docker-compose.yml -f docker-compose.streaming.yml ps`.
7. Suivre les logs : `docker compose -f docker-compose.yml -f docker-compose.streaming.yml logs -f spark-bronze-stream spark-silver-stream spark-gold-stream`.
8. Dans Airflow, déclencher `industrypulse_streaming_pipeline` après l’arrivée de nouvelles fenêtres Gold (quelques minutes de chauffe).

## Points de vigilance
- Les scripts d'origine contenaient des identifiants MinIO en clair : **changer ces identifiants** s'ils ont été exposés dans Git et ne jamais committer `.env`.
- Les scripts utilisent désormais le catalogue `lakehouse` cohérent avec Gold Sync ; vérifier que les tables historiques `industrypulse.*` et `lakehouse.*` pointent vers le même warehouse MinIO.
- `spark_gold_streaming.py` conserve une agrégation par fenêtre de 1 minute et un watermark de 2 minutes. Les résultats n'apparaissent pas instantanément.
- Le DAG contrôle le processus Spark via son UI et la fraîcheur via TimescaleDB. Il ne démarre pas lui-même les processus ; Docker Compose les maintient.
- Les fichiers n'ont pas été exécutés sur votre Docker Desktop : validation locale nécessaire.
- Le déclenchement à 5 minutes peut chevaucher le Batch : le Spark Runner existant verrouille un job à la fois et répond 409 si occupé.
- Pour rendre la commande unique `docker compose up -d` possible, fusionner ultérieurement les services de `docker-compose.streaming.yml` dans le compose principal.
