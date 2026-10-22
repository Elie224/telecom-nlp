"""Construit exports/annotation.csv : echantillon stratifie (~100 posts) a annoter.
Priorite aux posts a_verifier, puis a_analyser, equilibres par operateur_cible.
Python standard uniquement. Les posts deja annotes sont ignores. Lancer apres prepare_posts.py (et analyze_posts.py si dispo).
"""
import argparse
import csv
import hashlib
import sqlite3
from collections import defaultdict
from pathlib import Path

from pipeline_db import connect, safe_cell

COLUMNS = ['post_id', 'post_url', 'texte_original', 'texte_prepare', 'statut_auto',
           'motifs', 'operateur_cible_auto', 'sentiment_auto',
           'statut_valide', 'operateur_cible_valide', 'sentiment_valide', 'commentaire']


def quotas(counts, total):
    """Repartition equitable entre groupes (remplissage par niveau)."""
    q, left, open_ = {k: 0 for k in counts}, total, {k for k, n in counts.items() if n}
    while left > 0 and open_:
        share = max(1, left // len(open_))
        for k in sorted(open_):
            take = min(share, counts[k] - q[k], left)
            q[k] += take
            left -= take
            if q[k] >= counts[k]:
                open_.discard(k)
            if not left:
                break
    return q


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--taille', type=int, default=100)
    parser.add_argument('--graine', type=int, default=42)
    args = parser.parse_args()
    root = args.root.resolve()
    db = connect(root / 'data/telecom.sqlite3')
    db.row_factory = sqlite3.Row
    db.create_function('sample_key', 1,
                       lambda pid: hashlib.sha256(f'{args.graine}:{pid}'.encode()).hexdigest(),
                       deterministic=True)
    pred = '''(SELECT r.sentiment FROM predictions r WHERE r.post_id=p.post_id
               AND r.text_sha256=q.clean_sha256 ORDER BY r.analyzed_at DESC LIMIT 1)'''
    # Les posts deja annotes ne sont plus proposes. Echantillon deterministe (graine),
    # tri top-k dans SQLite : memoire bornee meme avec des millions de posts.
    not_done = 'AND NOT EXISTS (SELECT 1 FROM annotations a WHERE a.post_id=q.post_id)'
    chosen = []
    for status in ('a_verifier', 'a_analyser'):
        left = args.taille - len(chosen)
        if left <= 0:
            break
        counts = dict(db.execute(f'''SELECT COALESCE(operateur_cible,'indetermine'), COUNT(*)
            FROM prepared_posts q WHERE status=? {not_done}
            GROUP BY 1''', (status,)).fetchall())
        for cible, n in sorted(quotas(counts, left).items()):
            if not n:
                continue
            chosen += db.execute(f'''SELECT p.post_id, p.post_url, p.text, q.clean_text, q.status,
                q.flags, COALESCE(q.operateur_cible,'indetermine') AS cible, {pred} AS sentiment
                FROM prepared_posts q JOIN posts p ON p.post_id=q.post_id
                WHERE q.status=? AND COALESCE(q.operateur_cible,'indetermine')=? {not_done}
                ORDER BY sample_key(q.post_id) LIMIT ?''', (status, cible, n)).fetchall()
    (root / 'exports').mkdir(exist_ok=True)
    output = root / 'exports/annotation.csv'
    with output.open('w', newline='', encoding='utf-8-sig') as handle:
        writer = csv.writer(handle, quoting=csv.QUOTE_ALL)
        writer.writerow(COLUMNS)
        for r in chosen:
            writer.writerow([safe_cell(v) for v in (
                r['post_id'], r['post_url'], r['text'], r['clean_text'], r['status'],
                r['flags'], r['cible'], r['sentiment'] or '', '', '', '', '')])
    counts = defaultdict(int)
    for r in chosen:
        counts[(r['status'], r['cible'])] += 1
    print(f'{len(chosen)} publications exportees dans {output}')
    for (s, c), n in sorted(counts.items()):
        print(f'  {s:<11} {c:<18} {n}')
    print('Remplir statut_valide (a_analyser/exclu), operateur_cible_valide, '
          'sentiment_valide (negatif/neutre/positif), puis lancer import_annotation.py.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
