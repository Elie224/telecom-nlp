# Pipeline telecom-nlp

Ordre d'exécution (depuis le dossier `telecom-nlp`) :

1. `python import_posts.py` : importe les CSV de `collectes\` dans `data\telecom.sqlite3` (doublons ignorés grâce à `post_id`).
2. `python prepare_posts.py` : nettoyage v3, avec anonymisation (`<TEL>`, `<EMAIL>`, `<IBAN>`, `<NUM_CLIENT>`, `<ADRESSE>`), retrait des mentions et des liens, `texte_finetune` sans emojis, opérateur cible, doublons exacts et quasi-doublons, puis statut `a_analyser`, `a_verifier` ou `exclu`. Rapport dans `exports\preparation.csv` (20 000 lignes les plus récentes par défaut, `--export-limit -1` pour tout, `0` pour aucun).
3. `python analyze_posts.py --device cuda [--batch-size 64] [--max-tokens 16384]` : analyse les sentiments des posts `a_analyser` (modèle cardiffnlp). Résultats dans `exports\sentiments.csv`.
4. `python export_annotation.py [--taille 100]` : crée `exports\annotation.csv`, un échantillon stratifié qui commence par les `a_verifier`, avec le même nombre de posts par opérateur cible.
5. **Annoter à la main** dans Excel, puis enregistrer en CSV UTF-8 :
   - `statut_valide` : `a_analyser` ou `exclu`
   - `operateur_cible_valide` : `Orange`, `Free`, `SFR`, `Bouygues Telecom`, `indetermine` ou `aucun`
   - `sentiment_valide` : `negatif`, `neutre` ou `positif`
   - `commentaire` : libre
6. `python import_annotation.py` : vérifie les valeurs, enregistre la table `annotations` et affiche l'accord entre le résultat automatique et ton annotation.
7. `python build_dataset.py [--min-score 0.7] [--keep-unlabeled] [--split 80/10/10]` : construit le dataset final dans `dataset\final\<version>\`, recopié dans `dataset\final\latest\`.
   - Contenu : `train.jsonl`, `validation.jsonl` et `test.jsonl` ; les mêmes au format conversation (`*_instruct.jsonl`) ; `dataset.csv` ; `dataset_card.md` ; `stats.json`.
   - Posts retenus : tous les `a_analyser`, plus les `a_verifier` que tu as validés (`statut_valide=a_analyser`). Ceux que tu as exclus sont retirés.
   - Étiquette : ton annotation passe avant la prédiction du modèle (`label_source` = humain ou modele). Les posts sans étiquette sont retirés, sauf avec `--keep-unlabeled`.
   - Découpage stable par hachage de `post_id` : un post reste toujours dans le même split quand le dataset grandit. Copie traçable dans la table `dataset_final` (désactivable avec `--no-db-copy`).
   - Si les données n'ont pas changé, aucune nouvelle version n'est créée.

Les scripts 2, 3 et 7 partagent `data\pipeline.lock` : ne pas les lancer en même temps.
Après une mise à jour de `prepare_posts.py` (nouvelle version), tous les posts sont re-préparés et doivent être ré-analysés.

## Passage à l'échelle (millions de posts)

Tous les scripts partagent `pipeline_db.py` (à garder dans le même dossier) :
- SQLite en mode WAL, `synchronous=NORMAL`, cache 128 Mo, index sur `post_id`, `status`, `preparation_version`, `model_revision`, `created_at`, `operateur_cible`.
- Migrations automatiques et idempotentes, suivies dans la table `schema_version`. Une ancienne base est mise à niveau au premier lancement (colonne `posts.text_sha256`, table `minhash_bands`, index).

Fonctionnement incrémental et reprenable :
- Lecture et écriture par lots de 5 000 lignes, un commit par lot. Après un plantage ou un Ctrl+C, relancer la même commande : le travail déjà enregistré n'est pas refait.
- `import_posts.py` lit les CSV en flux (mémoire constante) ; un fichier reste tout ou rien.
- `prepare_posts.py` ne traite que les posts nouveaux, modifiés ou d'une ancienne version. Les quasi-doublons utilisent des signatures MinHash stockées dans `minhash_bands` : chaque nouveau post est comparé seulement à ses candidats (coût constant par post). Option `--batch-size`.
- `analyze_posts.py` : lots triés par longueur, taille de lot dynamique (budget `--max-tokens`, réduite automatiquement si la mémoire GPU sature), fp16 sur GPU (`--no-fp16` pour désactiver), `--chunk-size` posts par transaction, `--export-limit` pour `exports\sentiments.csv`.
- `export_annotation.py` tire l'échantillon dans SQLite (mémoire bornée) et ne repropose pas les posts déjà annotés.

`build_dataset.py`, options pour les gros volumes :
- `--max-examples N` : échantillon équilibré par opérateur et sentiment (déterministe). Conseillé pour le fine-tuning sur 8 Go (quelques dizaines de milliers d'exemples).
- `--shard-size 100000` : au-delà, les fichiers sont découpés (`train-00000.jsonl`, `train-00001.jsonl`…). En dessous, noms habituels (`train.jsonl`).
- `--csv-max-rows 1000000` : `dataset.csv` est limité (Excel) ; `0` = pas de CSV, `-1` = tout.
- `--keep-versions N` : ne garde que les N versions datées les plus récentes ; `--prune-only --keep-versions N` fait seulement le ménage.
- `latest\` utilise des liens physiques (pas de double occupation disque).

Ordre de grandeur mesuré (200 000 posts synthétiques, Linux) : import 2 s, préparation ~3,5 min la première fois puis < 1 s si rien de neuf, construction du dataset ~8 s, mémoire < 450 Mo. Après une nouvelle version de `prepare_posts.py`, tout est re-préparé une fois.
