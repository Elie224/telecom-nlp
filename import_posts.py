"""
Importe les CSV de Grok dans SQLite, sans dépendance externe.

Python 3.10 ou supérieur.

Fonctionnement :
- Lit les fichiers du dossier collectes.
- Ignore les doublons grâce à post_id.
- Archive les fichiers importés.
- Déplace les fichiers invalides dans erreurs.
- Écrit un journal dans logs/import.log.

Ce script ne collecte pas sur X et ne réentraîne pas de modèle.
"""

import argparse
import csv
import hashlib
import logging
import re
import sqlite3
import sys
import uuid

from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

from pipeline_db import BATCH_SIZE, connect, fingerprint


def maintenant():
    return datetime.now(timezone.utc).isoformat()


def normaliser(nom):
    return nom.strip().lower().replace(" ", "_")


COLONNES = {
    "post_id": ("post_id", "tweet_id", "id"),
    "post_url": ("post_url", "tweet_url", "url", "lien"),
    "text": ("text", "texte", "full_text", "contenu"),
    "created_at": ("created_at", "date", "date_publication"),
    "collected_at": ("collected_at", "date_collecte"),
    "lang": ("lang", "language", "langue"),
    "operators": (
        "operators", "operator", "operateur",
        "opérateur", "operateurs"
    ),
    "like_count": ("like_count", "likes"),
    "text_complete": ("text_complete",),
}


def lire_csv(flux):
    """Lit le CSV en flux (générateur) : mémoire constante même pour un gros fichier."""
    try:
        debut = flux.read(16384)
        flux.seek(0)
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Le fichier doit être enregistré en CSV UTF-8."
        ) from exc

    try:
        dialecte = csv.Sniffer().sniff(
            debut,
            delimiters=",;\t",
        )
    except csv.Error:
        dialecte = csv.excel

    lecteur = csv.DictReader(
        flux,
        dialect=dialecte,
        strict=True,
    )

    if not lecteur.fieldnames:
        raise ValueError("CSV vide ou sans en-tête.")

    entetes = [normaliser(nom) for nom in lecteur.fieldnames]

    if len(set(entetes)) != len(entetes):
        raise ValueError("Le CSV contient des colonnes dupliquées.")

    correspondances = {
        champ: next(
            (alias for alias in aliases if alias in entetes),
            None,
        )
        for champ, aliases in COLONNES.items()
    }

    if not correspondances["text"]:
        raise ValueError("Colonne obligatoire : text ou texte.")

    if not (
        correspondances["post_id"]
        or correspondances["post_url"]
    ):
        raise ValueError("Colonne obligatoire : post_id ou post_url.")

    nombre = 0

    for numero, ligne in enumerate(lecteur, start=2):
        if None in ligne or any(
            valeur is None for valeur in ligne.values()
        ):
            raise ValueError(
                f"Ligne {numero} : nombre de champs incorrect."
            )

        ligne = {
            normaliser(cle): valeur.strip()
            for cle, valeur in ligne.items()
        }

        if not any(ligne.values()):
            continue

        publication = {
            champ: ligne.get(colonne, "") if colonne else ""
            for champ, colonne in correspondances.items()
        }

        identifiant = publication["post_id"]

        correspondance_url = re.search(
            r"https://(?:www\.)?(?:x\.com|twitter\.com)"
            r"/[^/]+/status/(\d+)",
            publication["post_url"],
        )

        if not identifiant and correspondance_url:
            identifiant = correspondance_url.group(1)

        if not identifiant.isascii() or not identifiant.isdigit():
            raise ValueError(
                f"Ligne {numero} : identifiant X numérique invalide."
            )

        if (
            correspondance_url
            and correspondance_url.group(1) != identifiant
        ):
            raise ValueError(
                f"Ligne {numero} : post_id et URL incohérents."
            )

        if not publication["text"]:
            raise ValueError(f"Ligne {numero} : texte vide.")

        likes = publication["like_count"]

        if likes and (
            not likes.isascii() or not likes.isdigit()
        ):
            raise ValueError(
                f"Ligne {numero} : likes doit être "
                "un entier positif ou nul, ou rester vide."
            )

        publication["post_id"] = identifiant
        publication["like_count"] = int(likes) if likes else None

        nombre += 1
        yield publication

    if not nombre:
        raise ValueError("Aucune publication dans le fichier.")


def obtenir_verrou(chemin):
    """Empêche deux exécutions simultanées."""
    fichier = open(chemin, "a+b")
    fichier.seek(0)
    fichier.write(b"0")
    fichier.flush()
    fichier.seek(0)

    try:
        if sys.platform == "win32":
            import msvcrt
            msvcrt.locking(
                fichier.fileno(),
                msvcrt.LK_NBLCK,
                1,
            )
        else:
            import fcntl
            fcntl.flock(
                fichier,
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
    except OSError:
        fichier.close()
        return None

    return fichier


INSERT_SQL = """
    INSERT INTO posts (
        post_id, post_url, text, created_at, collected_at, lang, operators,
        like_count, text_complete, imported_at, source_file, text_sha256
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(post_id) DO NOTHING
"""


def empreinte_fichier(chemin):
    h = hashlib.sha256()
    with chemin.open("rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


def importer_fichier(connexion, chemin, empreinte):
    deja_importe = connexion.execute(
        "SELECT 1 FROM imports WHERE sha256 = ?",
        (empreinte,),
    ).fetchone()

    if deja_importe:
        return None

    ajouts = 0
    total = 0
    horodatage = maintenant()

    # Une erreur annule l'ensemble des insertions de ce fichier
    # (une seule transaction, insertions par lots de BATCH_SIZE).
    try:
        with connexion, chemin.open(encoding="utf-8-sig", newline="") as flux:
            lot = []

            def vider():
                nonlocal ajouts
                if lot:
                    avant = connexion.total_changes
                    connexion.executemany(INSERT_SQL, lot)
                    ajouts += connexion.total_changes - avant
                    lot.clear()

            for publication in lire_csv(flux):
                total += 1
                lot.append((
                    publication["post_id"], publication["post_url"],
                    publication["text"], publication["created_at"],
                    publication["collected_at"], publication["lang"],
                    publication["operators"], publication["like_count"],
                    publication["text_complete"], horodatage, chemin.name,
                    fingerprint(publication["text"]),
                ))
                if len(lot) >= BATCH_SIZE:
                    vider()
            vider()

            connexion.execute(
                """
                INSERT INTO imports (
                    sha256, filename, imported_at, rows_total, rows_inserted
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (empreinte, chemin.name, maintenant(), total, ajouts),
            )
    except UnicodeDecodeError as exc:
        raise ValueError(
            "Le fichier doit être enregistré en CSV UTF-8."
        ) from exc

    return ajouts, total - ajouts


def main():
    arguments = argparse.ArgumentParser(
        description="Import des CSV Grok dans SQLite."
    )

    arguments.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent,
        help="Dossier du projet. Par défaut : dossier du script.",
    )

    options = arguments.parse_args()
    racine = options.root.resolve()

    for dossier in (
        "collectes", "archives", "erreurs", "logs", "data"
    ):
        (racine / dossier).mkdir(parents=True, exist_ok=True)

    journal = logging.getLogger("import_posts")
    journal.setLevel(logging.INFO)

    gestionnaire = RotatingFileHandler(
        racine / "logs" / "import.log",
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )

    gestionnaire.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"
        )
    )

    journal.addHandler(gestionnaire)
    journal.addHandler(logging.StreamHandler())

    verrou = obtenir_verrou(racine / "data" / "import.lock")

    if verrou is None:
        journal.info("Un import est déjà en cours. Arrêt.")
        return 0

    erreurs = 0
    connexion = None

    try:
        # WAL, PRAGMA, index et migrations : voir pipeline_db.py.
        connexion = connect(racine / "data" / "telecom.sqlite3")

        fichiers = sorted(
            chemin
            for chemin in (racine / "collectes").iterdir()
            if chemin.is_file() and chemin.suffix.lower() == ".csv"
        )

        if not fichiers:
            journal.info("Aucun CSV à importer.")

        for chemin in fichiers:
            horodatage = datetime.now(timezone.utc).strftime(
                "%Y%m%dT%H%M%S"
            )

            nom_archive = (
                f"{horodatage}_"
                f"{uuid.uuid4().hex[:8]}_"
                f"{chemin.name}"
            )

            try:
                avant = chemin.stat()

                # Ignore les fichiers très récents pour laisser
                # le téléchargement ou la copie se terminer.
                age = datetime.now().timestamp() - avant.st_mtime

                if age < 10:
                    journal.info(
                        "Fichier récent, reporté : %s",
                        chemin.name,
                    )
                    continue

                empreinte = empreinte_fichier(chemin)
                apres = chemin.stat()

                if (
                    avant.st_size,
                    avant.st_mtime_ns,
                ) != (
                    apres.st_size,
                    apres.st_mtime_ns,
                ):
                    journal.info(
                        "Fichier en modification, reporté : %s",
                        chemin.name,
                    )
                    continue

                resultat = importer_fichier(
                    connexion,
                    chemin,
                    empreinte,
                )

                if resultat is None:
                    journal.info(
                        "Fichier déjà importé : %s",
                        chemin.name,
                    )
                else:
                    ajouts, doublons = resultat

                    journal.info(
                        "%s : %d ajouts, %d doublons.",
                        chemin.name,
                        ajouts,
                        doublons,
                    )

            except (ValueError, csv.Error) as exc:
                erreurs += 1

                journal.error(
                    "%s : %s",
                    chemin.name,
                    exc,
                )

                try:
                    destination = (
                        racine / "erreurs" / nom_archive
                    )

                    chemin.rename(destination)

                    destination.with_suffix(
                        destination.suffix + ".txt"
                    ).write_text(
                        str(exc),
                        encoding="utf-8",
                    )

                except OSError:
                    journal.exception(
                        "Impossible de déplacer le CSV invalide."
                    )

                continue

            except (OSError, sqlite3.Error):
                erreurs += 1

                journal.exception(
                    "Échec de l'import : %s. Fichier conservé.",
                    chemin.name,
                )
                continue

            # Archivage uniquement après validation en base.
            try:
                chemin.rename(
                    racine / "archives" / nom_archive
                )
            except OSError:
                erreurs += 1

                journal.exception(
                    "Échec d'archivage : %s. "
                    "Les données sont déjà enregistrées.",
                    chemin.name,
                )

        total = connexion.execute(
            "SELECT COUNT(*) FROM posts"
        ).fetchone()[0]

        journal.info(
            "Terminé : %d publications en base, %d erreurs.",
            total,
            erreurs,
        )

    except (OSError, sqlite3.Error):
        journal.exception("Erreur générale.")
        return 1

    finally:
        if connexion is not None:
            connexion.close()

        verrou.close()

    return 1 if erreurs else 0


if __name__ == "__main__":
    sys.exit(main())