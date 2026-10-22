"""Relit exports/annotation.csv rempli et l'enregistre dans la table annotations.
Valide les valeurs autorisees, puis affiche l'accord auto / valide.
Les lignes entierement vides (rien d'annote) sont ignorees.
"""
import argparse
import csv
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from pipeline_db import connect

STATUTS = {'a_analyser', 'exclu'}
OPERATEURS = {'Orange', 'Free', 'SFR', 'Bouygues Telecom', 'indetermine', 'aucun'}
SENTIMENTS = {'negatif', 'neutre', 'positif'}
ALIASES = {'bouygues': 'Bouygues Telecom', 'orange': 'Orange', 'free': 'Free',
           'sfr': 'SFR', 'négatif': 'negatif', 'positif': 'positif', 'indéterminé': 'indetermine'}


def norm(value):
    v = (value or '').strip().lstrip("'")
    return ALIASES.get(v.lower(), v)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--fichier', type=Path, default=None)
    args = parser.parse_args()
    root = args.root.resolve()
    path = args.fichier or root / 'exports/annotation.csv'
    db = connect(root / 'data/telecom.sqlite3')  # WAL + migrations (table annotations)
    db.execute('''CREATE TABLE IF NOT EXISTS annotations (
        post_id TEXT PRIMARY KEY REFERENCES posts(post_id),
        statut_valide TEXT CHECK(statut_valide IS NULL OR statut_valide IN ('a_analyser','exclu')),
        operateur_cible_valide TEXT,
        sentiment_valide TEXT CHECK(sentiment_valide IS NULL OR
            sentiment_valide IN ('negatif','neutre','positif')),
        commentaire TEXT,
        statut_auto TEXT, operateur_cible_auto TEXT, sentiment_auto TEXT,
        annotated_at TEXT NOT NULL)''')
    errors, records = [], []
    with path.open(encoding='utf-8-sig', newline='') as handle:
        for line, row in enumerate(csv.DictReader(handle), start=2):
            st, op, se = (norm(row.get(k)) for k in
                          ('statut_valide', 'operateur_cible_valide', 'sentiment_valide'))
            com = (row.get('commentaire') or '').strip()
            if not (st or op or se or com):
                continue
            if st and st not in STATUTS:
                errors.append(f'ligne {line} : statut_valide invalide « {st} »')
            if op and op not in OPERATEURS:
                errors.append(f'ligne {line} : operateur_cible_valide invalide « {op} »')
            if se and se not in SENTIMENTS:
                errors.append(f'ligne {line} : sentiment_valide invalide « {se} »')
            records.append((norm(row['post_id']), st or None, op or None, se or None,
                            com or None, row.get('statut_auto'), row.get('operateur_cible_auto'),
                            row.get('sentiment_auto') or None,
                            datetime.now(timezone.utc).isoformat()))
    if errors:
        print('Fichier refuse, rien n\'a ete enregistre :', *errors, sep='\n  ')
        return 1
    with db:
        db.executemany('''INSERT INTO annotations VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(post_id) DO UPDATE SET statut_valide=excluded.statut_valide,
            operateur_cible_valide=excluded.operateur_cible_valide,
            sentiment_valide=excluded.sentiment_valide, commentaire=excluded.commentaire,
            statut_auto=excluded.statut_auto, operateur_cible_auto=excluded.operateur_cible_auto,
            sentiment_auto=excluded.sentiment_auto, annotated_at=excluded.annotated_at''', records)
    print(f'{len(records)} annotations enregistrees.')
    if not records:
        print('Aucune ligne remplie : remplis les colonnes *_valide puis relance.')
        return 0

    def agree(auto_idx, valid_idx, mapper=lambda a: a):
        pairs = [(mapper(r[auto_idx]), r[valid_idx]) for r in records
                 if r[valid_idx] and r[auto_idx] and mapper(r[auto_idx]) is not None]
        if not pairs:
            return 'n/a (0 ligne)'
        ok = sum(a == v for a, v in pairs)
        return f'{ok}/{len(pairs)} = {ok / len(pairs):.0%}'
    # a_verifier n'est pas une decision : on compare la decision 'garder' vs 'exclure'.
    print('Accord statut (a_analyser vs exclu)  :',
          agree(5, 1, lambda a: 'exclu' if a == 'exclu' else 'a_analyser'))
    print('Accord sur les a_analyser auto seuls   :',
          agree(5, 1, lambda a: a if a == 'a_analyser' else None))
    print('Accord operateur cible               :', agree(6, 2))
    print('Accord sentiment                     :', agree(7, 3))
    return 0


if __name__ == '__main__':
    sys.exit(main())
