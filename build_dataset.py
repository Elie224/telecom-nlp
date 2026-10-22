"""Construit le dataset final d'entrainement depuis data/telecom.sqlite3.
A lancer apres prepare_posts.py, analyze_posts.py et import_annotation.py.
Python standard uniquement. Concu pour des millions de publications :
lecture en flux, ecriture en flux, fichiers decoupes en morceaux (shards).

Inclusion : statut a_analyser, plus les a_verifier valides par un humain
(statut_valide=a_analyser). Exclus : statut exclu et annotations statut_valide=exclu.
Etiquettes : annotation humaine > prediction du modele (version courante).
Decoupage : par hachage de post_id (stable quand le dataset grandit :
un post reste toujours dans le meme split).
"""
import argparse
import csv
import hashlib
import heapq
import json
import logging
import os
import shutil
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

from analyze_posts import MODEL_ID, PREPARATION_VERSION, PREPROCESS_VERSION, acquire_lock
from pipeline_db import BATCH_SIZE, connect, safe_cell, table_exists

SYSTEM = 'Tu analyses des avis clients sur les opérateurs télécom français.'
SPLITS = ('train', 'validation', 'test')
FIELDS = ['id', 'text', 'operateur_cible', 'operateurs_compares',
          'sentiment', 'label_source', 'score', 'created_at', 'lang', 'source',
          'preparation_version', 'model_id']
EXCEL_MAX_ROWS = 1_000_000


def unit_hash(post_id, salt):
    """Nombre deterministe dans [0, 1) derive de post_id."""
    h = hashlib.sha256(f'{salt}:{post_id}'.encode()).digest()
    return int.from_bytes(h[:8], 'big') / 2 ** 64


def split_of(post_id, ratios):
    x = unit_hash(post_id, 'split')
    if x < ratios[0]:
        return 'train'
    if x < ratios[0] + ratios[1]:
        return 'validation'
    return 'test'


def iter_records(db, min_score, skipped):
    """Genere les exemples retenus en flux, tries par post_id."""
    has_pred = table_exists(db, 'predictions')
    has_ann = table_exists(db, 'annotations')
    pred_cols = ('''(SELECT r.sentiment || '|' || r.score FROM predictions r
            WHERE r.post_id = p.post_id AND r.model_id = ? AND r.preprocessing_version = ?
              AND r.text_sha256 = q.clean_sha256
            ORDER BY r.analyzed_at DESC LIMIT 1) AS m_pred''' if has_pred else 'NULL AS m_pred')
    ann_sql = 'LEFT JOIN annotations a ON a.post_id = p.post_id' if has_ann else ''
    ann_cols = ('a.statut_valide AS a_statut, a.sentiment_valide AS a_sent, '
                'a.operateur_cible_valide AS a_op') if has_ann else \
        'NULL AS a_statut, NULL AS a_sent, NULL AS a_op'
    params = (MODEL_ID, PREPROCESS_VERSION) if has_pred else ()
    after = ''
    while True:
        rows = db.execute(f'''SELECT p.post_id, p.created_at, p.lang, q.status,
                q.text_finetune, q.operateur_cible, q.operateurs_compares,
                {pred_cols}, {ann_cols}
            FROM posts p JOIN prepared_posts q ON q.post_id = p.post_id {ann_sql}
            WHERE p.post_id > ? AND q.preparation_version = ? AND q.source_sha256 = p.text_sha256
            ORDER BY p.post_id LIMIT ?''',
                          (*params, after, PREPARATION_VERSION, BATCH_SIZE)).fetchall()
        if not rows:
            return
        after = rows[-1]['post_id']
        for r in rows:
            if r['status'] == 'exclu':
                skipped['statut_exclu'] += 1
                continue
            if r['a_statut'] == 'exclu':
                skipped['exclu_par_annotation'] += 1
                continue
            if r['status'] == 'a_verifier' and r['a_statut'] != 'a_analyser':
                skipped['a_verifier_non_valide'] += 1
                continue
            m_sent = m_score = None
            if r['m_pred']:
                m_sent, m_score = r['m_pred'].split('|')
                m_score = float(m_score)
            op = r['a_op'] or r['operateur_cible'] or 'indetermine'
            if r['a_sent']:
                sentiment, source, score, model = r['a_sent'], 'humain', None, None
            elif m_sent and m_score >= min_score:
                sentiment, source, score, model = m_sent, 'modele', m_score, MODEL_ID
            else:
                if m_sent:
                    skipped['score_modele_trop_bas'] += 1
                sentiment, source, score, model = None, 'aucun', None, None
            text = (r['text_finetune'] or '').strip()
            if not text:
                skipped['texte_vide'] += 1
                continue
            yield {
                'id': r['post_id'], 'text': text, 'operateur_cible': op,
                'operateurs_compares': [o for o in (r['operateurs_compares'] or '').split('|') if o],
                'sentiment': sentiment, 'label_source': source, 'score': score,
                'created_at': r['created_at'], 'lang': r['lang'], 'source': 'x',
                'preparation_version': PREPARATION_VERSION, 'model_id': model}


def balanced_selection(db, args):
    """1re passe : garde, par groupe (operateur, sentiment), les post_id de plus petit
    hachage. Quotas equilibres (remplissage par niveau). Memoire ~ max_examples."""
    heaps = defaultdict(list)  # groupe -> tas max (-hash, id) borne a max_examples
    cap = args.max_examples
    for rec in iter_records(db, args.min_score, Counter()):
        if rec['sentiment'] is None and not args.keep_unlabeled:
            continue
        g = (rec['operateur_cible'], str(rec['sentiment']))
        item = (-unit_hash(rec['id'], 'sample'), rec['id'])
        if len(heaps[g]) < cap:
            heapq.heappush(heaps[g], item)
        elif item > heaps[g][0]:
            heapq.heapreplace(heaps[g], item)
    sizes = {g: len(h) for g, h in heaps.items()}
    quota, remaining, open_groups = {g: 0 for g in sizes}, cap, set(sizes)
    while remaining > 0 and open_groups:
        share = max(1, remaining // len(open_groups))
        for g in sorted(open_groups):
            take = min(share, sizes[g] - quota[g], remaining)
            quota[g] += take
            remaining -= take
            if quota[g] >= sizes[g]:
                open_groups.discard(g)
            if remaining == 0:
                break
    keep = set()
    for g, h in heaps.items():
        keep.update(pid for _, pid in sorted(h, reverse=True)[:quota[g]])
    return keep, quota


def instruct(rec):
    return {'id': rec['id'], 'messages': [
        {'role': 'system', 'content': SYSTEM},
        {'role': 'user', 'content': f"Analyse cet avis : {rec['text']}"},
        {'role': 'assistant',
         'content': f"Opérateur : {rec['operateur_cible']}\nSentiment : {rec['sentiment']}"}]}


class ShardedWriter:
    """Ecrit name.jsonl, ou name-00000.jsonl, name-00001.jsonl... au-dela de shard_size lignes."""
    def __init__(self, folder, name, shard_size, digest):
        self.folder, self.name, self.size, self.digest = folder, name, shard_size, digest
        self.index, self.count, self.handle, self.files = -1, 0, None, []

    def _open(self):
        if self.handle:
            self.handle.close()
        self.index += 1
        path = self.folder / f'{self.name}-{self.index:05d}.jsonl'
        self.files.append(path)
        self.handle = path.open('w', encoding='utf-8', newline='\n')

    def write(self, obj):
        if self.handle is None or (self.size and self.count % self.size == 0 and self.count):
            self._open()
        line = json.dumps(obj, ensure_ascii=False) + '\n'
        self.handle.write(line)
        self.digest.update(line.encode('utf-8'))
        self.count += 1

    def close(self):
        if self.handle is None:
            self._open()
        self.handle.close()
        if len(self.files) == 1:  # un seul morceau : nom historique
            final = self.folder / f'{self.name}.jsonl'
            self.files[0].replace(final)
            self.files = [final]
        return [p.name for p in self.files]


def card(version, stats, args):
    lines = [f'# Dataset avis télécom (X) — version {version}', '',
             '## Provenance',
             '- Publications publiques en français collectées sur X (Orange, Free, SFR, Bouygues Telecom).',
             f"- Préparation : `{PREPARATION_VERSION}` ; modèle de pré-annotation : `{MODEL_ID}`.",
             f"- Généré le {stats['generated_at']} par build_dataset.py.", '',
             '## Effectifs',
             f"- Exemples retenus : {stats['total']} ; écartés : {json.dumps(stats['ecartes'], ensure_ascii=False)}.",
             f"- Sources d'étiquette : {json.dumps(stats['label_source'], ensure_ascii=False)}.",
             f"- Seuil de confiance modèle (--min-score) : {args.min_score} ; non étiquetés gardés : {args.keep_unlabeled}.",
             f"- Échantillon équilibré (--max-examples) : {args.max_examples or 'non (tout le dataset)'}.",
             f"- Fichiers : {json.dumps(stats['files'], ensure_ascii=False)}.", '',
             '## Découpage (stable, par hachage de post_id)', '',
             '| split | n | période | sentiments | opérateurs |', '|---|---|---|---|---|']
    for s in SPLITS:
        d = stats['splits'][s]
        lines.append(f"| {s} | {d['n']} | {d['debut']} → {d['fin']} | "
                     f"{json.dumps(d['sentiment'], ensure_ascii=False)} | "
                     f"{json.dumps(d['operateur'], ensure_ascii=False)} |")
    lines += ['', '## Nettoyage',
              '- Décodage HTML, normalisation Unicode NFC, suppression des caractères invisibles.',
              '- Suppression des mentions de début de réponse et des liens ; comptes officiels remplacés par la marque, autres comptes par @user.',
              '- `text` : sans emojis ni symbole #, sans @user (fine-tuning). Aucun emoji dans le dataset final.',
              '- Doublons exacts et quasi-doublons (MinHash LSH, Jaccard 3-grammes ≥ 0,8) exclus ; hors sujet et textes trop courts exclus.', '',
              '## Anonymisation',
              '- Téléphones → `<TEL>`, e-mails → `<EMAIL>`, IBAN → `<IBAN>`, numéros client/contrat/dossier → `<NUM_CLIENT>`, adresses → `<ADRESSE>`.',
              '- Les identifiants de posts (id) restent présents : ne pas publier le dataset tel quel.', '',
              '## Limites',
              "- Étiquettes « modele » = prédictions automatiques non vérifiées ; seules « humain » sont validées.",
              "- Opérateur cible obtenu par règles ; `indetermine` quand plusieurs opérateurs sans règle sûre.",
              "- Découpage aléatoire stable (hachage) : un post ne change jamais de split quand le dataset grandit.",
              "- Respecter les conditions d'utilisation de X et le RGPD pour tout usage."]
    return '\n'.join(lines) + '\n'


def link_or_copy(src, dst):
    try:
        os.link(src, dst)  # lien physique : pas de double occupation disque (NTFS ok)
    except OSError:
        shutil.copy2(src, dst)


def publish_latest(root, out):
    latest = root / 'dataset/final/latest'
    tmp = latest.with_name('latest.tmp')
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir()
    for f in out.iterdir():
        link_or_copy(f, tmp / f.name)
    shutil.rmtree(latest, ignore_errors=True)
    tmp.rename(latest)
    return latest


def prune_versions(root, keep, log):
    """Garde les `keep` versions datees les plus recentes (latest n'est jamais supprime)."""
    base = root / 'dataset/final'
    versions = sorted(p for p in base.iterdir()
                      if p.is_dir() and p.name[:8].isdigit() and 'T' in p.name)
    removed = []
    for p in versions[:-keep] if keep > 0 else []:
        shutil.rmtree(p, ignore_errors=True)
        removed.append(p.name)
    if removed:
        log.info('Retention : %d anciennes versions supprimees (%s).', len(removed), ', '.join(removed))
    return removed


def build(db, root, args, log):
    ratios = [float(x) for x in args.split.split('/')]
    ratios = [x / sum(ratios) for x in ratios]
    keep, quota = (balanced_selection(db, args) if args.max_examples else (None, None))
    now = datetime.now(timezone.utc)
    version = now.strftime('%Y%m%dT%H%M%SZ')
    base = root / 'dataset/final'
    for stale in base.glob('.en_cours_*'):  # restes d'une construction interrompue
        shutil.rmtree(stale, ignore_errors=True)
    out = base / version
    suffix = 1
    while out.exists():  # deux builds dans la meme seconde
        suffix += 1
        out = base / f'{version}_{suffix}'
    version = out.name
    work = base / f'.en_cours_{version}'
    work.mkdir(parents=True)
    digest = hashlib.sha256()
    writers = {s: ShardedWriter(work, s, args.shard_size, digest) for s in SPLITS}
    inst = {s: ShardedWriter(work, f'{s}_instruct', args.shard_size, hashlib.sha256()) for s in SPLITS}
    skipped = Counter()
    split_stats = {s: {'n': 0, 'debut': None, 'fin': None, 'sentiment': Counter(),
                       'operateur': Counter()} for s in SPLITS}
    label_source = Counter()
    csv_rows = 0
    csv_handle = None
    if args.csv_max_rows != 0:
        csv_handle = (work / 'dataset.csv').open('w', encoding='utf-8-sig', newline='')
        cw = csv.writer(csv_handle, quoting=csv.QUOTE_ALL)
        cw.writerow(['split'] + FIELDS)
    db_rows = []
    if args.db_copy:
        db.execute('''CREATE TABLE IF NOT EXISTS dataset_final (
            version TEXT NOT NULL, split TEXT NOT NULL, post_id TEXT NOT NULL,
            text TEXT NOT NULL, operateur_cible TEXT, sentiment TEXT, label_source TEXT,
            score REAL, preparation_version TEXT, model_id TEXT, built_at TEXT NOT NULL,
            PRIMARY KEY (version, post_id))''')
        db.execute('CREATE INDEX IF NOT EXISTS ix_dsf_post ON dataset_final(post_id)')
        db.execute('DELETE FROM dataset_final WHERE version=?', (version,))
        db.commit()
    db_sql = 'INSERT OR REPLACE INTO dataset_final VALUES (?,?,?,?,?,?,?,?,?,?,?)'
    total = 0
    try:
        for rec in iter_records(db, args.min_score, skipped):
            if rec['sentiment'] is None and not args.keep_unlabeled:
                skipped['sans_etiquette'] += 1
                continue
            if keep is not None and rec['id'] not in keep:
                skipped['hors_echantillon_equilibre'] += 1
                continue
            s = split_of(rec['id'], ratios)
            writers[s].write(rec)
            if rec['sentiment'] is not None:
                inst[s].write(instruct(rec))
            d = split_stats[s]
            d['n'] += 1
            c = rec['created_at']
            if c:
                d['debut'] = c if d['debut'] is None or c < d['debut'] else d['debut']
                d['fin'] = c if d['fin'] is None or c > d['fin'] else d['fin']
            d['sentiment'][str(rec['sentiment'])] += 1
            d['operateur'][rec['operateur_cible']] += 1
            label_source[rec['label_source']] += 1
            total += 1
            if csv_handle and (args.csv_max_rows < 0 or csv_rows < args.csv_max_rows):
                cw.writerow([s] + [safe_cell('|'.join(rec[f]) if f == 'operateurs_compares' else rec[f])
                                   for f in FIELDS])
                csv_rows += 1
            if args.db_copy:
                db_rows.append((version, s, rec['id'], rec['text'], rec['operateur_cible'],
                                rec['sentiment'], rec['label_source'], rec['score'],
                                rec['preparation_version'], rec['model_id'], now.isoformat()))
                if len(db_rows) >= BATCH_SIZE:
                    with db:
                        db.executemany(db_sql, db_rows)
                    db_rows.clear()
            if total % 100000 == 0:
                log.info('  %d exemples ecrits...', total)
        files = {}
        for s in SPLITS:
            files[s] = writers[s].close()
            files[f'{s}_instruct'] = inst[s].close()
        if db_rows:
            with db:
                db.executemany(db_sql, db_rows)
            db_rows.clear()
    except BaseException:
        if args.db_copy:
            db.rollback()
            with db:
                db.execute('DELETE FROM dataset_final WHERE version=?', (version,))
        raise
    finally:
        if csv_handle:
            csv_handle.close()
    content = digest.hexdigest()
    latest = base / 'latest'
    old = latest / 'stats.json'
    if old.exists() and json.loads(old.read_text(encoding='utf-8')).get('content_sha256') == content:
        shutil.rmtree(work, ignore_errors=True)
        if args.db_copy:
            with db:
                db.execute('DELETE FROM dataset_final WHERE version=?', (version,))
        log.info('Dataset inchange (%d exemples) : rien a reconstruire. Voir %s', total, latest)
        return {'total': total, 'splits': {s: {'n': split_stats[s]['n']} for s in SPLITS}}, latest, False
    if csv_handle:
        files['dataset.csv'] = ['dataset.csv']
    stats = {'version': version, 'generated_at': now.isoformat(), 'total': total,
             'ecartes': dict(skipped), 'label_source': dict(label_source),
             'preparation_version': PREPARATION_VERSION, 'split_method': 'sha256(post_id)',
             'csv_rows': csv_rows, 'csv_tronque': bool(csv_handle) and csv_rows < total,
             'max_examples': args.max_examples, 'quotas': {f'{k[0]}|{k[1]}': v for k, v in (quota or {}).items()},
             'files': files, 'content_sha256': content,
             'splits': {s: {**split_stats[s], 'sentiment': dict(split_stats[s]['sentiment']),
                            'operateur': dict(split_stats[s]['operateur'])} for s in SPLITS}}
    (work / 'stats.json').write_text(json.dumps(stats, ensure_ascii=False, indent=2), encoding='utf-8')
    (work / 'dataset_card.md').write_text(card(version, stats, args), encoding='utf-8')
    work.rename(out)
    publish_latest(root, out)
    if args.keep_versions:
        removed = prune_versions(root, args.keep_versions, log)
        if removed and args.db_copy:
            with db:
                db.executemany('DELETE FROM dataset_final WHERE version=?', [(v,) for v in removed])
    return stats, out, True


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--min-score', type=float, default=0.0,
                        help='Score minimal pour garder une etiquette du modele.')
    parser.add_argument('--keep-unlabeled', action='store_true',
                        help='Garder les exemples sans etiquette (sentiment vide).')
    parser.add_argument('--split', default='80/10/10', help='Proportions train/validation/test.')
    parser.add_argument('--max-examples', type=int, default=0,
                        help='Taille max, echantillon equilibre par operateur et sentiment (0 = tout).')
    parser.add_argument('--shard-size', type=int, default=100000,
                        help='Lignes max par fichier jsonl (0 = un seul fichier).')
    parser.add_argument('--csv-max-rows', type=int, default=EXCEL_MAX_ROWS,
                        help='Lignes max dans dataset.csv (-1 = tout, 0 = pas de CSV).')
    parser.add_argument('--keep-versions', type=int, default=0,
                        help='Ne garder que les N versions datees les plus recentes (0 = tout garder).')
    parser.add_argument('--no-db-copy', dest='db_copy', action='store_false',
                        help='Ne pas copier le dataset dans la table dataset_final.')
    parser.add_argument('--prune-only', action='store_true',
                        help='Appliquer seulement --keep-versions, sans reconstruire.')
    args = parser.parse_args()
    if not 0 <= args.min_score <= 1:
        parser.error('--min-score doit etre entre 0 et 1.')
    if args.max_examples < 0 or args.shard_size < 0 or args.keep_versions < 0:
        parser.error('--max-examples, --shard-size et --keep-versions doivent etre positifs.')
    if args.prune_only and not args.keep_versions:
        parser.error('--prune-only demande --keep-versions N.')
    try:
        parts = [float(x) for x in args.split.split('/')]
        assert len(parts) == 3 and all(p >= 0 for p in parts) and sum(parts) > 0
    except (ValueError, AssertionError):
        parser.error('--split attend trois nombres, ex. 80/10/10.')
    root = args.root.resolve()
    for name in ('logs', 'dataset/final'):
        (root / name).mkdir(parents=True, exist_ok=True)
    log = logging.getLogger('build_dataset')
    log.setLevel(logging.INFO)
    fh = logging.FileHandler(root / 'logs/build_dataset.log', encoding='utf-8')
    fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    log.addHandler(fh)
    log.addHandler(logging.StreamHandler())
    path = root / 'data/telecom.sqlite3'
    if not path.exists():
        log.error('Base absente : lance import_posts.py en premier.')
        return 1
    lock = acquire_lock(root / 'data/pipeline.lock')
    if lock is None:
        log.info('Une preparation, analyse ou construction est deja en cours.')
        return 0
    db = None
    try:
        db = connect(path)
        db.row_factory = __import__('sqlite3').Row
        if args.prune_only:
            removed = prune_versions(root, args.keep_versions, log)
            if removed and table_exists(db, 'dataset_final'):
                with db:
                    db.executemany('DELETE FROM dataset_final WHERE version=?', [(v,) for v in removed])
            return 0
        if not db.execute('SELECT 1 FROM prepared_posts LIMIT 1').fetchone():
            log.error('Lance prepare_posts.py avant build_dataset.py.')
            return 1
        stats, out, built = build(db, root, args, log)
        if built:
            log.info('Dataset %s : %d exemples (train %d / validation %d / test %d). Sources : %s. Ecartes : %s',
                     stats['version'], stats['total'], *(stats['splits'][s]['n'] for s in SPLITS),
                     stats['label_source'], stats['ecartes'])
            log.info('Sortie : %s (copie dans dataset/final/latest)', out)
        if stats['total'] == 0:
            log.warning('Aucun exemple etiquete : lance analyze_posts.py ou annote, '
                        'ou utilise --keep-unlabeled.')
        return 0
    except Exception:
        log.exception('Construction interrompue.')
        return 1
    finally:
        if db is not None:
            db.close()
        lock.close()


if __name__ == '__main__':
    raise SystemExit(main())
