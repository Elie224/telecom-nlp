"""Acces SQLite partage par tous les scripts du pipeline telecom-nlp.

- Connexion avec WAL et PRAGMA adaptes aux gros volumes.
- Migrations idempotentes suivies dans la table schema_version.
- Index sur toutes les colonnes de filtre et de jointure.
Python standard uniquement.
"""
import hashlib
import sqlite3
from datetime import datetime, timezone

BATCH_SIZE = 5000  # lignes par lot / transaction


def fingerprint(text):
    return hashlib.sha256((text or '').encode('utf-8')).hexdigest()


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def connect(path, timeout=60):
    db = sqlite3.connect(path, timeout=timeout)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=NORMAL')      # sur en WAL, beaucoup plus rapide
    db.execute('PRAGMA temp_store=MEMORY')
    db.execute('PRAGMA cache_size=-131072')      # ~128 Mo de cache
    db.execute('PRAGMA mmap_size=268435456')     # 256 Mo
    db.execute('PRAGMA busy_timeout=60000')
    db.execute('PRAGMA foreign_keys=ON')
    db.create_function('text_hash', 1, fingerprint, deterministic=True)
    migrate(db)
    return db


def table_exists(db, name):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                      (name,)).fetchone() is not None


def columns(db, table):
    return {r[1] for r in db.execute(f'PRAGMA table_info({table})')}


def add_column(db, table, name, decl):
    if table_exists(db, table) and name not in columns(db, table):
        db.execute(f'ALTER TABLE {table} ADD COLUMN {name} {decl}')


# ------------------------------------------------------------------ migrations
def _m1_base(db):
    db.executescript('''
    CREATE TABLE IF NOT EXISTS posts (
        post_id TEXT PRIMARY KEY, post_url TEXT, text TEXT NOT NULL,
        created_at TEXT, collected_at TEXT, lang TEXT, operators TEXT,
        like_count INTEGER, text_complete TEXT,
        imported_at TEXT NOT NULL, source_file TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS imports (
        sha256 TEXT PRIMARY KEY, filename TEXT NOT NULL, imported_at TEXT NOT NULL,
        rows_total INTEGER NOT NULL, rows_inserted INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS prepared_posts (
        post_id TEXT PRIMARY KEY REFERENCES posts(post_id),
        source_sha256 TEXT NOT NULL, clean_text TEXT NOT NULL,
        clean_sha256 TEXT NOT NULL, preparation_version TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('a_analyser','a_verifier','exclu')),
        flags TEXT NOT NULL, duplicate_of TEXT, prepared_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS predictions (
        post_id TEXT NOT NULL REFERENCES posts(post_id),
        model_id TEXT NOT NULL, model_revision TEXT NOT NULL,
        preprocessing_version TEXT NOT NULL, text_sha256 TEXT NOT NULL,
        sentiment TEXT NOT NULL CHECK(sentiment IN ('negatif','neutre','positif')),
        score REAL NOT NULL, score_negatif REAL NOT NULL, score_neutre REAL NOT NULL,
        score_positif REAL NOT NULL, analyzed_at TEXT NOT NULL,
        PRIMARY KEY (post_id, model_id, model_revision, preprocessing_version, text_sha256));
    CREATE TABLE IF NOT EXISTS annotations (
        post_id TEXT PRIMARY KEY REFERENCES posts(post_id),
        statut_valide TEXT CHECK(statut_valide IS NULL OR statut_valide IN ('a_analyser','exclu')),
        operateur_cible_valide TEXT,
        sentiment_valide TEXT CHECK(sentiment_valide IS NULL OR
            sentiment_valide IN ('negatif','neutre','positif')),
        commentaire TEXT, statut_auto TEXT, operateur_cible_auto TEXT, sentiment_auto TEXT,
        annotated_at TEXT NOT NULL);
    ''')
    for name in ('text_finetune', 'operateur_cible', 'operateurs_compares'):
        add_column(db, 'prepared_posts', name, 'TEXT')


def _m2_scaling(db):
    add_column(db, 'posts', 'text_sha256', 'TEXT')
    db.execute('UPDATE posts SET text_sha256 = text_hash(text) WHERE text_sha256 IS NULL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS pipeline_state (key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS minhash_bands (
        band_key INTEGER NOT NULL, post_id TEXT NOT NULL,
        PRIMARY KEY (band_key, post_id)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS ix_minhash_post ON minhash_bands(post_id);
    CREATE INDEX IF NOT EXISTS ix_posts_created ON posts(created_at, post_id);
    CREATE INDEX IF NOT EXISTS ix_prep_version_status ON prepared_posts(preparation_version, status, post_id);
    CREATE INDEX IF NOT EXISTS ix_prep_status ON prepared_posts(status);
    CREATE INDEX IF NOT EXISTS ix_prep_clean ON prepared_posts(clean_sha256);
    CREATE INDEX IF NOT EXISTS ix_prep_cible ON prepared_posts(operateur_cible);
    CREATE INDEX IF NOT EXISTS ix_pred_model ON predictions(model_id, preprocessing_version, post_id, analyzed_at);
    CREATE INDEX IF NOT EXISTS ix_pred_revision ON predictions(model_revision);
    ''')
    if table_exists(db, 'dataset_final'):
        db.execute('CREATE INDEX IF NOT EXISTS ix_dsf_post ON dataset_final(post_id)')


MIGRATIONS = [(1, _m1_base), (2, _m2_scaling)]


def migrate(db):
    db.execute('CREATE TABLE IF NOT EXISTS schema_version (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)')
    done = {r[0] for r in db.execute('SELECT version FROM schema_version')}
    for version, func in MIGRATIONS:
        if version in done:
            continue
        with db:
            func(db)
            db.execute('INSERT OR IGNORE INTO schema_version VALUES (?,?)', (version, now_iso()))
    db.commit()


def get_state(db, key, default=None):
    row = db.execute('SELECT value FROM pipeline_state WHERE key=?', (key,)).fetchone()
    return row[0] if row else default


def set_state(db, key, value):
    db.execute('INSERT INTO pipeline_state VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
               (key, value))


def safe_cell(value):
    """Protege l'ouverture dans Excel (formules =, +, - non numeriques)."""
    if isinstance(value, str) and value.lstrip()[:1] in ('=', '+', '-') \
            and not value.lstrip()[1:2].isdigit():
        return "'" + value
    return value
