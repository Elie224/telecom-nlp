"""
Analyse les publications de data/telecom.sqlite3.

- Exécution locale : aucun appel à Grok ou X.
- Téléchargement du modèle au premier lancement.
- Enregistrement des résultats dans la table predictions.
- Les publications déjà analysées avec la même version sont ignorées.
- Export des résultats dans exports/sentiments.csv.
- Aucune modification des publications originales.
- Aucun entraînement automatique.

Python 3.10 ou supérieur.
"""

import argparse
import csv
import hashlib
import logging
import os
import sqlite3
import sys
import time

from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pipeline_db import BATCH_SIZE, connect, fingerprint, migrate


MODEL_ID = "cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual"
PREPARATION_VERSION = "preparation-v3"
PREPROCESS_VERSION = "preparation-v3-mentions-urls-max256"

LABELS = {
    "negative": "negatif",
    "neutral": "neutre",
    "positive": "positif",
}


def preprocess(text):
    """Normalise les mentions et les liens sans retirer les emojis."""
    return " ".join(
        "@user"
        if token.startswith("@") and len(token) > 1
        else "http"
        if token.startswith("http")
        else token
        for token in text.split(" ")
    )


def acquire_lock(path):
    """Empêche deux analyses de s'exécuter simultanément."""
    handle = open(path, "a+b")
    handle.seek(0)
    handle.write(b"0")
    handle.flush()
    handle.seek(0)

    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(
                handle.fileno(),
                msvcrt.LK_NBLCK,
                1,
            )
        else:
            import fcntl

            fcntl.flock(
                handle,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )

    except OSError:
        handle.close()
        return None

    return handle


def init_schema(db):
    """Tables et index : migrations idempotentes de pipeline_db.py."""
    migrate(db)


def pending(db, revision, after, limit):
    """Sélectionne un lot de publications restant à analyser (pagination par post_id)."""
    return db.execute(
        """
        SELECT p.post_id, q.clean_text, q.clean_sha256
        FROM posts p
        JOIN prepared_posts q ON q.post_id = p.post_id
        WHERE p.post_id > ?
          AND q.status = 'a_analyser'
          AND q.preparation_version = ?
          AND q.source_sha256 = p.text_sha256
          AND NOT EXISTS (
            SELECT 1 FROM predictions r
            WHERE r.post_id = p.post_id
              AND r.model_id = ?
              AND r.model_revision = ?
              AND r.preprocessing_version = ?
              AND r.text_sha256 = q.clean_sha256
          )
        ORDER BY p.post_id
        LIMIT ?
        """,
        (after, PREPARATION_VERSION, MODEL_ID, revision, PREPROCESS_VERSION, limit),
    ).fetchall()


def process(db, predict, revision, chunk_size, logger):
    """Lit la base par lots de chunk_size, trie par longueur (moins de padding),
    prédit, puis enregistre le lot dans une transaction : reprise possible à tout moment."""
    after = ""
    total = 0
    started = time.monotonic()

    while True:
        rows = pending(db, revision, after, chunk_size)
        if not rows:
            break

        order = sorted(range(len(rows)), key=lambda i: len(rows[i][1]))
        texts = [preprocess(rows[i][1]) for i in order]
        sorted_results = predict(texts)

        if len(sorted_results) != len(rows):
            raise ValueError(
                "Le modèle a retourné un nombre de résultats incorrect."
            )

        results = [None] * len(rows)
        for i, res in zip(order, sorted_results):
            results[i] = res

        stamp = datetime.now(timezone.utc).isoformat()
        values = []
        for (post_id, text, clean_sha), scores in zip(rows, results):
            if set(scores) != set(LABELS.values()):
                raise ValueError("Classes du modèle inattendues.")
            if (
                any(not 0 <= score <= 1 for score in scores.values())
                or abs(sum(scores.values()) - 1) > 0.001
            ):
                raise ValueError("Scores du modèle invalides.")
            sentiment = max(scores, key=scores.get)
            values.append((
                post_id, MODEL_ID, revision, PREPROCESS_VERSION,
                clean_sha or fingerprint(text), sentiment, scores[sentiment],
                scores["negatif"], scores["neutre"], scores["positif"], stamp,
            ))

        # Chaque lot est enregistré dans une transaction.
        with db:
            db.executemany(
                """
                INSERT INTO predictions (
                    post_id, model_id, model_revision, preprocessing_version,
                    text_sha256, sentiment, score, score_negatif, score_neutre,
                    score_positif, analyzed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                """,
                values,
            )

        total += len(rows)
        after = rows[-1][0]
        rate = total / max(time.monotonic() - started, 1e-6)
        logger.info(
            "%d publications analysées pendant cette exécution (%.0f/s).",
            total, rate,
        )

    return total


def export_results(db, revision, output, threshold, limit=-1):
    """Exporte les résultats correspondant aux textes actuels (limit lignes les plus récentes, -1 = tout)."""
    temporary = output.with_suffix(".csv.tmp")

    try:
        with temporary.open(
            "w",
            newline="",
            encoding="utf-8-sig",
        ) as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL)

            writer.writerow(
                [
                    "post_id",
                    "operators",
                    "text",
                    "sentiment",
                    "score",
                    "score_negatif",
                    "score_neutre",
                    "score_positif",
                    "a_verifier",
                    "model_id",
                    "model_revision",
                    "analyzed_at",
                ]
            )

            rows = db.execute(
                """
                SELECT
                    p.post_id,
                    p.operators,
                    q.clean_text,
                    r.sentiment,
                    r.score,
                    r.score_negatif,
                    r.score_neutre,
                    r.score_positif,
                    r.score < ?,
                    r.model_id,
                    r.model_revision,
                    r.analyzed_at

                FROM posts p
                JOIN prepared_posts q ON q.post_id = p.post_id
                JOIN predictions r ON r.post_id = p.post_id

                WHERE q.status = 'a_analyser'
                  AND q.preparation_version = ?
                  AND q.source_sha256 = p.text_sha256
                  AND r.model_id = ?
                  AND r.model_revision = ?
                  AND r.preprocessing_version = ?
                  AND r.text_sha256 = q.clean_sha256

                ORDER BY p.post_id DESC
                LIMIT ?
                """,
                (
                    threshold,
                    PREPARATION_VERSION,
                    MODEL_ID,
                    revision,
                    PREPROCESS_VERSION,
                    limit,
                ),
            )

            for row in rows:
                # Protège l'ouverture du CSV dans Excel.
                # Seules les vraies formules (=, +, - non numerique) sont
                # protegees ; plus d'apostrophe devant les @.
                safe_row = [
                    "'" + value
                    if isinstance(value, str)
                    and value.lstrip()[:1] in ("=", "+", "-")
                    and not value.lstrip()[1:2].isdigit()
                    else value
                    for value in row
                ]

                writer.writerow(safe_row)

        temporary.replace(output)

    finally:
        temporary.unlink(missing_ok=True)


def load_predictor(root, offline, revision_option, device_option,
                   max_batch=64, max_tokens=16384, fp16=True):
    import torch

    from transformers import (
        AutoConfig,
        AutoTokenizer,
        AutoModelForSequenceClassification,
    )

    options = {
        "cache_dir": str(root / "model_cache"),
        "local_files_only": offline,
        "trust_remote_code": False,
    }

    from huggingface_hub import hf_hub_download

    config_path = hf_hub_download(
        repo_id=MODEL_ID,
        filename="config.json",
        revision=revision_option,
        cache_dir=str(root / "model_cache"),
        local_files_only=offline,
    )
    revision = Path(config_path).parent.name

    if len(revision) != 40 or any(
        c not in "0123456789abcdef" for c in revision.lower()
    ):
        raise RuntimeError(f"Révision du modèle non reconnue : {revision}")

    config = AutoConfig.from_pretrained(
        MODEL_ID,
        revision=revision,
        **options,
    )

    tokenizer = AutoTokenizer.from_pretrained(
        MODEL_ID,
        revision=revision,
        **options,
    )

    model = AutoModelForSequenceClassification.from_pretrained(
        MODEL_ID,
        revision=revision,
        use_safetensors=None,
        **options,
    )

    labels = {
        int(index): LABELS.get(label.lower())
        for index, label in config.id2label.items()
    }

    if set(labels.values()) != set(LABELS.values()) or len(labels) != 3:
        raise RuntimeError(
            f"Classes non reconnues : {config.id2label}"
        )

    device = device_option
    if device == "cuda" and not torch.cuda.is_available():
        logging.getLogger("analyse").warning(
            "CUDA indisponible : repli sur le processeur."
        )
        device = "cpu"

    use_fp16 = fp16 and device == "cuda"
    if use_fp16:
        model.half()  # fp16 sur GPU : ~2x plus rapide, moitié de mémoire
    model.to(device)
    model.eval()
    log = logging.getLogger("analyse")
    state = {"max_batch": max_batch}

    def run(batch):
        encoded = tokenizer(
            batch, padding=True, truncation=True, max_length=256,
            return_tensors="pt",
        )
        encoded = {key: value.to(device) for key, value in encoded.items()}
        with torch.inference_mode():
            logits = model(**encoded).logits.float()
            return torch.softmax(logits, dim=-1).cpu().tolist()

    def predict(texts):
        """Textes déjà triés par longueur. Taille de lot dynamique :
        budget de tokens (lot x longueur) et division par deux en cas de saturation mémoire GPU."""
        lengths = [min(len(ids), 256) for ids in tokenizer(
            texts, truncation=True, max_length=256)["input_ids"]]
        probabilities = []
        i = 0
        while i < len(texts):
            size = 1
            while (i + size < len(texts) and size < state["max_batch"]
                   and (size + 1) * max(lengths[i:i + size + 1]) <= max_tokens):
                size += 1
            try:
                probabilities.extend(run(texts[i:i + size]))
                i += size
            except torch.cuda.OutOfMemoryError:
                torch.cuda.empty_cache()
                if size == 1:
                    raise
                state["max_batch"] = max(1, size // 2)
                log.warning("Mémoire GPU saturée : lot réduit à %d.", state["max_batch"])
        return [
            {labels[index]: float(score) for index, score in enumerate(row)}
            for row in probabilities
        ]

    return predict, revision, device


def main():
    parser = argparse.ArgumentParser(
        description="Analyse locale des sentiments."
    )

    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Taille maximale d'un lot GPU/CPU (réduite automatiquement si besoin).",
    )

    parser.add_argument(
        "--max-tokens",
        type=int,
        default=16384,
        help="Budget de tokens par lot (lot x longueur). 16384 convient à 8 Go en fp16.",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=BATCH_SIZE,
        help="Publications lues et enregistrées par transaction.",
    )

    parser.add_argument(
        "--no-fp16",
        action="store_true",
        help="Désactiver le fp16 sur GPU.",
    )

    parser.add_argument(
        "--export-limit",
        type=int,
        default=20000,
        help="Lignes max dans exports/sentiments.csv (-1 = tout, 0 = aucun export).",
    )

    parser.add_argument(
        "--review-threshold",
        type=float,
        default=0.70,
    )

    parser.add_argument(
        "--device",
        choices=["cpu", "cuda"],
        default="cpu",
    )

    parser.add_argument(
        "--revision",
        default="main",
        help="Révision Hugging Face. Utiliser un SHA pour figer la version.",
    )

    parser.add_argument(
        "--offline",
        action="store_true",
        help="Utiliser uniquement le modèle déjà téléchargé.",
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size doit être supérieur à zéro.")

    if not 0 <= args.review_threshold <= 1:
        parser.error("--review-threshold doit être entre 0 et 1.")

    root = args.root.resolve()

    for name in ("logs", "exports", "data"):
        (root / name).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("analyse")
    logger.setLevel(logging.INFO)

    handler = RotatingFileHandler(
        root / "logs" / "analyze.log",
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )

    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
    )

    logger.addHandler(handler)
    logger.addHandler(logging.StreamHandler())

    database_path = root / "data" / "telecom.sqlite3"

    if not database_path.exists():
        logger.error(
            "Base absente : lance import_posts.py en premier."
        )
        return 1

    lock = acquire_lock(root / "data" / "pipeline.lock")

    if lock is None:
        logger.info("Une analyse est déjà en cours.")
        return 0

    db = None

    try:
        db = connect(database_path)

        if not db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='prepared_posts'"
        ).fetchone():
            logger.error("Lance prepare_posts.py avant cette analyse.")
            return 1

        init_schema(db)

        total_posts = db.execute(
            """SELECT COUNT(*) FROM posts p JOIN prepared_posts q
               ON q.post_id=p.post_id WHERE q.status='a_analyser'
               AND q.preparation_version=?
               AND q.source_sha256=p.text_sha256""",
            (PREPARATION_VERSION,),
        ).fetchone()[0]

        if total_posts == 0:
            logger.info("Aucune publication preparee admissible. Voir exports/preparation.csv.")
            export_path = root / "exports" / "sentiments.csv"
            with export_path.open("w", newline="", encoding="utf-8-sig") as empty:
                csv.writer(empty).writerow([
                    "post_id", "operators", "text", "sentiment", "score",
                    "score_negatif", "score_neutre", "score_positif", "a_verifier",
                    "model_id", "model_revision", "analyzed_at"
                ])
            return 0

        logger.info(
            "Chargement du modèle. Le premier téléchargement "
            "peut prendre plusieurs minutes."
        )

        predict, revision, device = load_predictor(
            root,
            args.offline,
            args.revision,
            args.device,
            args.batch_size,
            args.max_tokens,
            not args.no_fp16,
        )

        logger.info(
            "Modèle : %s | révision : %s | appareil : %s",
            MODEL_ID,
            revision,
            device,
        )

        total = process(
            db,
            predict,
            revision,
            args.chunk_size,
            logger,
        )

        output = root / "exports" / "sentiments.csv"

        if args.export_limit != 0:
            export_results(
                db,
                revision,
                output,
                args.review_threshold,
                args.export_limit,
            )

        logger.info(
            "Terminé : %d nouvelles analyses. Export : %s",
            total,
            output,
        )

        return 0

    except Exception:
        logger.exception(
            "Analyse interrompue. Les lots déjà enregistrés "
            "sont conservés."
        )
        return 1

    finally:
        if db is not None:
            db.close()

        lock.close()


if __name__ == "__main__":
    _code = main() or 0
    sys.stdout.flush(); sys.stderr.flush()
    logging.shutdown()
    os._exit(int(_code))  # evite le blocage a la fermeture (CUDA/threads)