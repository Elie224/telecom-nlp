"""Preparation des publications importees, sans modifier posts.
Python standard uniquement. v3 (passage a l'echelle : incremental, par lots, LSH persistant) : anonymisation, operateur cible, quasi-doublons
(MinHash/LSH), en plus du nettoyage v2. Lancer avant analyze_posts.py.
Les regles signalent des cas a verifier ; elles ne comprennent pas la cible
reelle d'une critique. Les predictions ne sont jamais des annotations validees.
"""
import argparse
import csv
import hashlib
import html
import json
import logging
import re
import sqlite3
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from pipeline_db import BATCH_SIZE, connect, get_state, safe_cell, set_state

VERSION = 'preparation-v3'

# Comptes officiels -> nom de marque (gardes dans le texte comme information utile).
OFFICIAL_HANDLES = {
    'orange_france': 'Orange', 'orange': 'Orange', 'orange_conseil': 'Orange',
    'orange_sav': 'Orange', 'sosh': 'Sosh', 'sfr': 'SFR', 'sfr_sav': 'SFR',
    'redbysfr': 'RED by SFR', 'free': 'Free', 'freebox': 'Freebox',
    'freemobile': 'Free Mobile', 'bouyguestelecom': 'Bouygues Telecom',
    'bouyguestelecomsav': 'Bouygues Telecom', 'bandyou': 'B&You',
}
OPERATOR_WORDS = re.compile(
    r'\b(?:orange|sosh|livebox|sfr|red by sfr|free|freebox|free mobile|'
    r'bouygues|bbox|b&you|op[ée]rateur)\b', re.I)
LEADING_MENTIONS = re.compile(r'^(?:\s*@\w+)+\s*')
URL = re.compile(r'https?://\S+|www\.\S+', re.I)
ZERO_WIDTH = re.compile('[\u200b-\u200f\u2060-\u2064\ufeff\u00ad]')
HANDLE = re.compile(r'(?<![\w@])@(\w{1,30})')
# Emojis / pictogrammes / drapeaux / selecteurs de variante / ZWJ.
EMOJI = re.compile('[\U0000203C\U00002049\U00002122\U00002139\U00003030\U0000303D\U00003297\U00003299\U0001F000-\U0001FAFF\U00002600-\U000027BF\U00002B00-\U00002BFF'
                   '\U0001F1E6-\U0001F1FF\U0000FE00-\U0000FE0F\U0000200D\U000020E3'
                   '\U00002190-\U000021FF\U00002300-\U000023FF\U000025A0-\U000025FF'
                   '\U000E0020-\U000E007F]')
INSULTS = re.compile(
    r'\b(?:connard|connasse|con|conne|abruti|d[ée]bile|cr[ée]tin|idiot|imb[ée]cile|'
    r'ta gueule|ferme[- ]la|fdp|pd|encul[ée]|b[aâ]tard|ch[ôo]meur|clown|guignol|'
    r'boloss?|tocard|merde|putain|nique|salope)\b', re.I)
DISPUTE = re.compile(
    r"\b(?:je (?:ne )?t'ai pas (?:sonn[ée]|parl[ée])|t'es qui|occupe[- ]toi|"
    r"on t'a pas sonn[ée]|va te faire|retourne)\b", re.I)
SERVICE_WORDS = re.compile(
    r'\b(?:r[ée]seau|box|fibre|forfait|client|service|panne|facture|d[ée]bit|'
    r'connexion|abonnement|r[ée]sili|4g|5g|internet|t[ée]l[ée]phone|mobile|'
    r'offre|prix|conseiller|sav)\b', re.I)


def fingerprint(text):
    return hashlib.sha256((text or '').encode('utf-8')).hexdigest()


# ---------------------------------------------------------------- v3 : anonymisation
EMAIL = re.compile(r'[\w.+-]+@[\w-]+(?:\.[\w-]+)+')
IBAN = re.compile(r'\b[A-Z]{2}\d{2}(?:\s?[A-Z0-9]{4}){3,7}(?:\s?[A-Z0-9]{1,4})?\b')
PHONE = re.compile(r'(?<![\w+])(?:(?:\+|00)33\s?\(?0?\)?\s?[1-9]|0[1-9])(?:[\s.-]?\d{2}){4}(?!\d)')
NUM_CLIENT = re.compile(
    r"\b(?:n[°o]\.?\s*(?:de\s+)?(?:client|contrat|dossier|ticket|commande|ligne)?|"
    r"num[ée]ro\s+(?:de\s+)?(?:client|contrat|dossier|ticket|commande)|"
    r"r[ée]f(?:[ée]rence)?\.?|dossier|ticket|contrat|identifiant|id\s*client)"
    r"\s*[:#]?\s*(?=[A-Z0-9-]*\d)[A-Z0-9][A-Z0-9-]{3,}\b", re.I)
LONG_DIGITS = re.compile(r'(?<![\d,.])\d{8,}(?![\d,.])')
ADDRESS = re.compile(
    r"\b\d{1,4}\s?(?:bis|ter)?,?\s+(?:rue|avenue|av\.|bd|boulevard|all[ée]e|impasse|"
    r"chemin|place|quai|route|cours|square|r[ée]sidence)\s+(?:(?:de|du|des|la|le|l'|d')\s*)*"
    r"[A-ZÀ-Ÿ][\w'’-]*(?:\s+[A-ZÀ-Ÿ][\w'’-]*){0,3}(?:,?\s+\d{5}(?:\s+[A-ZÀ-Ÿ][\w-]*)?)?", re.I)


def anonymize(text):
    text = EMAIL.sub('<EMAIL>', text)
    text = IBAN.sub('<IBAN>', text)
    text = NUM_CLIENT.sub('<NUM_CLIENT>', text)   # avant TEL : 'contrat n°06...'
    text = PHONE.sub('<TEL>', text)
    text = ADDRESS.sub('<ADRESSE>', text)
    return LONG_DIGITS.sub('<NUM_CLIENT>', text)


# ---------------------------------------------------------------- v3 : operateur cible
BRANDS = {
    'Orange': r"orange|sosh|livebox",
    'Free': r"free\s*mobile|freebox|free",
    'SFR': r"red by sfr|sfr",
    'Bouygues Telecom': r"bouygues(?:\s*t[ée]l[ée]com)?|bbox|b&you",
}
BRAND_RE = {op: re.compile(r"(?<![\w@])(?:" + p + r")\b", re.I) for op, p in BRANDS.items()}
HANDLE_OPERATOR = {
    'orange_france': 'Orange', 'orange': 'Orange', 'orange_conseil': 'Orange',
    'orange_sav': 'Orange', 'sosh': 'Orange', 'sfr': 'SFR', 'sfr_sav': 'SFR',
    'redbysfr': 'SFR', 'free': 'Free', 'freebox': 'Free', 'freemobile': 'Free',
    'bouyguestelecom': 'Bouygues Telecom', 'bouyguestelecomsav': 'Bouygues Telecom',
    'bandyou': 'Bouygues Telecom'}
_ART = r"(?:chez\s+|de\s+chez\s+|l'|d'|le\s+|la\s+|de\s+)?"
# (regle, gabarit, role de X) : role 'cible' = X est critique/quitte ; 'autre' = X sert de comparaison
SWITCH_RULES = [
    ('quitter', r"(?:quitt\w*|r[ée]sili\w*|partir\s+de\s+chez|fuir)\s+" + _ART + "{X}", 'cible'),
    ('passer_chez', r"(?:je\s+|j'|on\s+)?(?:passe|pars|suis\s+pass[ée]e?|vais|vais\s+passer|"
                    r"pars|partir|aller|retourne)\s+(?:chez|sur|à|a)\s+{X}", 'autre'),
    ('chez_x_mieux', r"chez\s+{X}\s*,?\s*(?:c'est|c’est|tout)\s+(?:mieux|bien|nickel|parfait|top|fonctionne)", 'autre'),
    ('contrairement', r"(?:contrairement\s+à|au\s+contraire\s+de|par\s+rapport\s+à)\s+" + _ART + "{X}", 'autre'),
    ('x_est_mieux', r"{X}\s+(?:est|c'est|c’est)\s+(?:bien\s+|beaucoup\s+|largement\s+)?(?:mieux|meilleur|top|au\s+top)", 'autre'),
    ('pire_que', r"(?:pire|moins\s+bien|plus\s+nul|pas\s+mieux)\s+que\s+" + _ART + "{X}", 'autre'),
]


def mentioned_operators(text):
    found = []
    for op, rx in BRAND_RE.items():
        m = rx.search(text)
        if m:
            found.append((m.start(), op))
    return [op for _, op in sorted(found)]


def target_operator(raw_text, clean, declared):
    """Retourne (cible, compares, regle ou None, confiance_haute)."""
    ops = mentioned_operators(clean)
    for op in declared:
        if op not in ops and op in BRANDS:
            ops.append(op)
    if not ops:
        return 'indetermine', '', None, False
    if len(ops) == 1:
        return ops[0], '', 'operateur_unique', True
    low = clean.lower()
    for name, template, role in SWITCH_RULES:
        hits = {op for op in ops if re.search(
            template.replace('{X}', '(?:' + BRANDS[op] + r')\b'), low)}
        if len(hits) == 1:
            x = hits.pop()
            if role == 'cible':
                cible = x
            elif len(ops) == 2:
                cible = next(o for o in ops if o != x)
            else:
                continue
            return cible, '|'.join(o for o in ops if o != cible), name, True
    lead = LEADING_MENTIONS.match(html.unescape(raw_text or ''))
    addressed = {HANDLE_OPERATOR[h.lower()] for h in re.findall(r'@(\w+)', lead.group(0))
                 if h.lower() in HANDLE_OPERATOR} if lead else set()
    addressed &= set(ops)
    if len(addressed) == 1:
        cible = addressed.pop()
        return cible, '|'.join(o for o in ops if o != cible), 'compte_officiel_interpelle', True
    # Premier operateur cite : indice faible, la cible reste a verifier.
    return 'indetermine', '|'.join(ops), 'premiere_mention:' + ops[0], False


# ---------------------------------------------------------------- v3 : quasi-doublons
NEAR_THRESHOLD = 0.8
MINHASH_PERM = 32
MINHASH_BANDS = 16  # 16 bandes x 2 lignes : rappel eleve des J >= 0.8
_SEEDS = [int.from_bytes(hashlib.blake2b(str(i).encode(), digest_size=8).digest(), 'big')
          for i in range(MINHASH_PERM)]


def shingles(text):
    words = re.findall(r"\w+", text.lower())
    return {' '.join(words[i:i + 3]) for i in range(len(words) - 2)}


def _h(s):
    return int.from_bytes(hashlib.blake2b(s.encode(), digest_size=8).digest(), 'big')


def minhash(sh):
    hs = [_h(s) for s in sh]
    return [min((x ^ seed) for x in hs) for seed in _SEEDS]


MAX_CANDIDATES = 10    # candidats verifies par Jaccard exact (les plus de bandes communes)
MIN_SHARED_BANDS = 2   # J>=0.8 partage ~10 bandes sur 16 en moyenne
BUCKET_CAP = 1000      # lignes lues au plus par seau (formules tres generiques)


def band_keys(sig):
    """16 cles entieres 64 bits signees (bande + valeurs), stockees dans minhash_bands."""
    rows = MINHASH_PERM // MINHASH_BANDS
    keys = []
    for b in range(MINHASH_BANDS):
        raw = repr((b, sig[b * rows:(b + 1) * rows])).encode()
        keys.append(int.from_bytes(hashlib.blake2b(raw, digest_size=8).digest(), 'big', signed=True))
    return keys


class NearDupIndex:
    """LSH MinHash persistant (table minhash_bands) : comparaison incrementale,
    seuls les candidats partageant une bande sont relus, puis Jaccard exact."""
    def __init__(self, db):
        self.db = db

    def find(self, sh):
        if len(sh) < 3:
            return None, None
        keys = band_keys(minhash(sh))
        # Candidats classes par nombre de bandes partagees (>= 2), seaux bornes :
        # cout constant par post, meme avec des millions de signatures.
        union = ' UNION ALL '.join(
            f'SELECT * FROM (SELECT post_id FROM minhash_bands WHERE band_key=? LIMIT {BUCKET_CAP})'
            for _ in keys)
        cands = self.db.execute(
            f'''SELECT c.post_id, q.text_finetune FROM (
                   SELECT post_id, COUNT(*) AS n FROM ({union}) GROUP BY post_id
                   HAVING n >= {MIN_SHARED_BANDS} ORDER BY n DESC, post_id LIMIT {MAX_CANDIDATES}) c
               JOIN prepared_posts q ON q.post_id = c.post_id
               WHERE q.preparation_version = ? AND q.duplicate_of IS NULL''',
            (*keys, VERSION)).fetchall()
        best = (None, 0.0)
        for cand, text in sorted(cands, key=lambda r: r[0]):
            other = shingles(text or '')
            if not other:
                continue
            j = len(sh & other) / len(sh | other)
            if j > best[1]:
                best = (cand, j)
        return (best[0] if best[1] >= NEAR_THRESHOLD else None), keys

    def add(self, post_id, keys):
        if keys is None:
            return
        self.db.executemany('INSERT OR IGNORE INTO minhash_bands VALUES (?,?)',
                            [(k, post_id) for k in keys])


def _replace_handle(match):
    return OFFICIAL_HANDLES.get(match.group(1).lower(), '@user')


def clean_text(text):
    """Texte pour le modele de sentiment : emojis conserves."""
    text = html.unescape(text or '')
    text = unicodedata.normalize('NFC', text)
    text = ZERO_WIDTH.sub('', text)
    text = LEADING_MENTIONS.sub('', text)      # mentions de la chaine de reponse
    text = URL.sub(' ', text)
    text = anonymize(text)                     # TEL, EMAIL, IBAN, NUM_CLIENT, ADRESSE
    text = HANDLE.sub(_replace_handle, text)   # autres @ -> @user ou marque
    return re.sub(r'\s+', ' ', text).strip()


def finetune_text(clean):
    """Version pour le fine-tuning : sans emojis ni symbole #, sans @user."""
    text = EMOJI.sub(' ', clean)
    text = re.sub(r'#(\w+)', r'\1', text)
    text = re.sub(r'@user\b', ' ', text)
    text = re.sub(r'\s+([,.;:!?…)])', r'\1', text)
    return re.sub(r'\s+', ' ', text).strip()


def inspect_row(row, text):
    """Retourne (flags, exclu). exclu=True pour les cas sans interet."""
    flags = []
    exclude = False
    words = re.findall(r"[^\W\d_][\w'’-]*", EMOJI.sub(' ', text))
    if not OPERATOR_WORDS.search(text):
        lead = LEADING_MENTIONS.match(html.unescape(row['text'] or ''))
        handles = {h.lower() for h in re.findall(r'@(\w+)', lead.group(0))} if lead else set()
        if handles & OFFICIAL_HANDLES.keys() and SERVICE_WORDS.search(text):
            # Reponse a un compte officiel parlant du service : on garde a verifier.
            flags.append('operateur_seulement_en_mention')
        else:
            flags.append('hors_sujet_operateur')
            exclude = True
    if len(words) < 4:
        flags.append('texte_trop_court')
        exclude = True
    if INSULTS.search(text) and not SERVICE_WORDS.search(text):
        flags.append('insulte_ou_dispute_personnelle')
    elif DISPUTE.search(text):
        flags.append('dispute_personnelle_possible')
    raw = (row['text'] or '').strip()
    if re.match(r'^[a-zà-ÿ]', text) and re.match(r'^(?:et|mais|ou|donc|car|puis|qui|que|alors|en plus|sauf)\b', text):
        flags.append('fragment_de_fil_possible')
    elif len(raw) >= 260 and not re.search(r'[.!?…»)"\]]\s*$', EMOJI.sub('', text).strip()):
        flags.append('fragment_de_fil_possible')
    complete = str(row['text_complete'] or '').strip().lower()
    if complete in {'false', '0', 'non', 'no'}:
        flags.append('texte_incomplet_declare')
    elif text.endswith(('...', '…')):
        flags.append('fin_en_points_de_suspension')
    lang = str(row['lang'] or '').strip().lower()
    if lang and lang not in {'fr', 'fra', 'fre', 'french', 'francais', 'français'}:
        flags.append('langue_a_verifier')
    if not str(row['operators'] or '').strip() and not mentioned_operators(text):
        flags.append('operateur_non_renseigne')
    if re.search(r'#(?:concours|codepromo|promotion)\b|code promo|offre sponsoris', text, re.I):
        flags.append('promotion_possible')
    if re.search(r'(?:est|disponible) sur (?:la|le|les canaux|le canal)\s*\d+', text, re.I):
        flags.append('annonce_tv_possible')
    return flags, exclude


UPSERT = '''INSERT INTO prepared_posts (post_id, source_sha256,
    clean_text, clean_sha256, preparation_version, status, flags,
    duplicate_of, prepared_at, text_finetune, operateur_cible,
    operateurs_compares) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(post_id) DO UPDATE SET
        source_sha256=excluded.source_sha256,
        clean_text=excluded.clean_text, clean_sha256=excluded.clean_sha256,
        preparation_version=excluded.preparation_version,
        status=excluded.status, flags=excluded.flags,
        duplicate_of=excluded.duplicate_of, prepared_at=excluded.prepared_at,
        text_finetune=excluded.text_finetune,
        operateur_cible=excluded.operateur_cible,
        operateurs_compares=excluded.operateurs_compares'''


def prepare_one(db, row, near):
    original = row['text'] or ''
    text = clean_text(original)
    clean_hash = fingerprint(text)
    flags, exclude = inspect_row(row, text)
    ft = finetune_text(text)
    declared = [o.strip() for o in str(row['operators'] or '').split('|') if o.strip()]
    cible, compares, rule, sure = target_operator(original, text, declared)
    if compares or (cible == 'indetermine' and len(declared) > 1):
        if sure:
            flags.append('cible_resolue_par_regle:' + rule)
        else:
            flags.append('plusieurs_operateurs_cible_a_verifier')
            if rule:
                flags.append('cible_probable_' + rule)
    duplicate_of = None
    keys = None
    # Representant deja present (meme texte normalise, version courante, non doublon).
    seen = db.execute('''SELECT post_id FROM prepared_posts WHERE clean_sha256=?
        AND preparation_version=? AND duplicate_of IS NULL AND post_id != ?
        ORDER BY post_id LIMIT 1''', (clean_hash, VERSION, row['post_id'])).fetchone() if text else None
    if not text:
        status = 'exclu'
        flags.append('texte_vide')
    elif seen:
        status = 'exclu'
        duplicate_of = seen[0]
        flags.append('doublon_texte_normalise')
    else:
        sh = shingles(ft)
        near_of, keys = near.find(sh)
        if near_of is not None and near_of != row['post_id']:
            status = 'exclu'
            duplicate_of = near_of
            keys = None
            flags.append('quasi_doublon')
        elif exclude:
            status = 'exclu'
        else:
            blocking = [f for f in flags if not f.startswith('cible_resolue_par_regle')]
            status = 'a_verifier' if blocking else 'a_analyser'
    db.execute('DELETE FROM minhash_bands WHERE post_id=?', (row['post_id'],))
    db.execute(UPSERT, (row['post_id'], row['text_sha256'] or fingerprint(original), text,
                        clean_hash, VERSION, status, json.dumps(flags, ensure_ascii=False),
                        duplicate_of, datetime.now(timezone.utc).isoformat(), ft,
                        cible, compares))
    near.add(row['post_id'], keys)
    return status


def prepare(db, batch_size=BATCH_SIZE, log=None):
    """Incremental et reprenable : ne traite que les posts nouveaux, modifies
    ou d'une ancienne version ; un commit par lot de batch_size posts."""
    db.row_factory = sqlite3.Row
    if get_state(db, 'preparation_version') != VERSION:
        # Nouvelle version des regles : index LSH remis a zero, tout sera re-prepare.
        # Premiere execution apres mise a jour : les posts deja prepares en VERSION
        # recoivent leurs signatures (par lots), les autres seront re-prepares.
        with db:
            db.execute('DELETE FROM minhash_bands')
        after = ''
        while True:
            rows = db.execute('''SELECT post_id, text_finetune FROM prepared_posts
                WHERE post_id > ? AND preparation_version = ? AND duplicate_of IS NULL
                ORDER BY post_id LIMIT ?''', (after, VERSION, batch_size)).fetchall()
            if not rows:
                break
            with db:
                for r in rows:
                    sh = shingles(r['text_finetune'] or '')
                    if len(sh) >= 3:
                        NearDupIndex(db).add(r['post_id'], band_keys(minhash(sh)))
            after = rows[-1]['post_id']
        with db:
            set_state(db, 'preparation_version', VERSION)
    near = NearDupIndex(db)
    counts = Counter()
    changed = 0
    after = ''
    while True:
        rows = db.execute('''SELECT p.post_id, p.text, p.text_sha256, p.lang, p.operators,
                p.text_complete FROM posts p
            LEFT JOIN prepared_posts q ON q.post_id = p.post_id
            WHERE p.post_id > ? AND (q.post_id IS NULL OR q.preparation_version != ?
                                     OR q.source_sha256 != p.text_sha256)
            ORDER BY p.post_id LIMIT ?''', (after, VERSION, batch_size)).fetchall()
        if not rows:
            break
        with db:
            for row in rows:
                counts[prepare_one(db, row, near)] += 1
        changed += len(rows)
        after = rows[-1]['post_id']
        if log:
            log.info('  %d publications preparees...', changed)
    with db:
        db.execute('''DELETE FROM minhash_bands WHERE post_id IN (SELECT post_id FROM prepared_posts q
            WHERE NOT EXISTS (SELECT 1 FROM posts p WHERE p.post_id = q.post_id))''')
        db.execute('''DELETE FROM prepared_posts WHERE NOT EXISTS
            (SELECT 1 FROM posts p WHERE p.post_id = prepared_posts.post_id)''')
    totals = Counter(dict(db.execute('''SELECT status, COUNT(*) FROM prepared_posts
        WHERE preparation_version=? GROUP BY status''', (VERSION,)).fetchall()))
    return totals, changed


def export_preparation(db, output, limit):
    """limit : nombre max de lignes (les plus recentes) ; -1 = tout ; 0 = pas d'export."""
    temp = output.with_suffix('.csv.tmp')
    try:
        with temp.open('w', newline='', encoding='utf-8-sig') as handle:
            writer = csv.writer(handle, quoting=csv.QUOTE_ALL)
            writer.writerow(['post_id', 'operators', 'texte_original', 'texte_prepare',
                             'texte_finetune', 'operateur_cible', 'operateurs_compares',
                             'statut', 'motifs', 'doublon_de'])
            rows = db.execute('''SELECT p.post_id,p.operators,p.text,q.clean_text,
                q.text_finetune,q.operateur_cible,q.operateurs_compares,q.status,q.flags,q.duplicate_of FROM posts p
                JOIN prepared_posts q ON q.post_id=p.post_id ORDER BY p.post_id DESC LIMIT ?''', (limit,))
            for row in rows:
                # Pas d'apostrophe pour les @ : seules les vraies formules
                # (=, +, - suivis d'un caractere non numerique) sont protegees.
                writer.writerow([safe_cell(v) for v in row])
        temp.replace(output)
    finally:
        temp.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help='Posts par lot (un commit par lot).')
    parser.add_argument('--export-limit', type=int, default=20000,
                        help='Lignes max dans exports/preparation.csv (-1 = tout, 0 = aucun export).')
    args = parser.parse_args()
    root = args.root.resolve()
    for name in ('exports', 'logs'):
        (root / name).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[
        logging.StreamHandler(),
        logging.FileHandler(root / 'logs/prepare.log', encoding='utf-8')])
    path = root / 'data/telecom.sqlite3'
    if not path.exists():
        logging.error('Base absente : lance import_posts.py en premier.')
        return 1
    # Verrouillage et utilitaires partages avec le script d'analyse fourni.
    from analyze_posts import acquire_lock
    lock = acquire_lock(root / 'data/pipeline.lock')
    if lock is None:
        logging.info('Une preparation ou analyse est deja en cours.')
        return 0
    db = None
    try:
        db = connect(path)
        counts, changed = prepare(db, args.batch_size, logging.getLogger())
        logging.info('Preparation : %d a analyser, %d a verifier, %d exclus. %d lignes actualisees.',
                     counts['a_analyser'], counts['a_verifier'], counts['exclu'], changed)
        if args.export_limit != 0:
            export_preparation(db, root / 'exports/preparation.csv', args.export_limit)
            logging.info('Rapport (%s lignes max) : %s',
                         'toutes' if args.export_limit < 0 else args.export_limit,
                         root / 'exports/preparation.csv')
        return 0
    except Exception:
        logging.exception('Preparation interrompue.')
        return 1
    finally:
        if db is not None:
            db.close()
        lock.close()


if __name__ == '__main__':
    raise SystemExit(main())
