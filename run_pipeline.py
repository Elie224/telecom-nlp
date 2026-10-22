"""Pipeline automatique telecom-nlp.

Enchaine : import des CSV de collectes/ -> preparation -> analyse de sentiment
-> dataset final -> export public (IDs + labels + stats, sans texte)
-> fine-tuning si assez de nouveaux exemples (seuil).

Usage :
    python run_pipeline.py                  # run normal
    python run_pipeline.py --force          # tout relancer meme sans nouveau CSV
    python run_pipeline.py --no-train       # ne jamais lancer le fine-tuning
    python run_pipeline.py --train-threshold 2000
"""
import argparse
import csv
import json
import re
import logging
import os
import sqlite3
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PY = sys.executable
STEPS = [
    ("import", ["import_posts.py"]),
    ("preparation", ["prepare_posts.py"]),
    ("analyse", ["analyze_posts.py", "--device", "{device}"]),
    ("dataset", ["build_dataset.py", "--keep-versions", "5"]),
]


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def setup_log(root):
    (root / "logs").mkdir(exist_ok=True)
    log = logging.getLogger("run_pipeline")
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    for h in (logging.FileHandler(root / "logs" / "run_pipeline.log", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        log.addHandler(h)
    return log


class Lock:
    """Empeche deux runs en meme temps (Planificateur + lancement manuel)."""

    def __init__(self, path, max_age_h=12):
        self.path, self.max_age = path, max_age_h * 3600

    def __enter__(self):
        if self.path.exists() and time.time() - self.path.stat().st_mtime < self.max_age:
            raise SystemExit(f"Un run est deja en cours ({self.path}). Supprime ce fichier s'il est perime.")
        self.path.write_text(str(os.getpid()), encoding="utf-8")
        return self

    def __exit__(self, *exc):
        self.path.unlink(missing_ok=True)


def load_state(path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"last_train_examples": 0, "last_train_at": None, "runs": 0}


def run_step(name, cmd, root, log, timeout_h):
    full = [PY, "-u"] + [str(root / cmd[0])] + cmd[1:] + ["--root", str(root)]
    log.info("=== %s : %s", name, " ".join(full))
    t0 = time.time()
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    proc = subprocess.run(full, cwd=root, env=env, timeout=timeout_h * 3600)
    log.info("%s termine en %.0fs (code %s)", name, time.time() - t0, proc.returncode)
    if proc.returncode != 0:
        raise RuntimeError(f"Etape {name} en echec (code {proc.returncode})")


def latest_version(db):
    try:
        row = db.execute("SELECT MAX(version) FROM dataset_final").fetchone()
    except sqlite3.OperationalError:
        return None
    return row[0] if row else None


SUJETS = {
    "reseau": r"r[ée]seau|4g|5g|antenne|couverture|d[ée]bit|connexion|coupure|panne|capte|signal|bug",
    "fibre_box": r"fibre|box|livebox|freebox|bbox|wifi|wi-fi|adsl|raccord|installation",
    "prix": r"prix|tarif|cher|euros?|€|factur|augment|promo|offre|abonnement|forfait|rembours|frais",
    "service_client": r"service client|sav|conseiller|hotline|t[ée]l[ée]phon|r[ée]ponse|attente|r[ée]clamation|ticket|joindre|support",
    "resiliation": r"r[ée]sili|quitter|partir|changer d.op[ée]rateur|portabilit|rio",
}
SUJETS_RE = {k: re.compile(v, re.I) for k, v in SUJETS.items()}


def sujets_de(text):
    found = [k for k, rx in SUJETS_RE.items() if rx.search(text or "")]
    return found or ["autre"]


def export_public(root, log):
    """IDs + labels + stats agregees. Jamais de texte (regles X)."""
    out = root / "public"
    out.mkdir(exist_ok=True)
    db = sqlite3.connect(root / "data" / "telecom.sqlite3")
    try:
        version = latest_version(db)
        if not version:
            log.warning("Pas de dataset_final : export public ignore.")
            return 0
        rows = db.execute(
            """SELECT d.post_id, d.split, d.operateur_cible, d.sentiment, d.label_source,
                      ROUND(d.score, 4), substr(p.created_at, 1, 10), d.text
               FROM dataset_final d LEFT JOIN posts p ON p.post_id = d.post_id
               WHERE d.version = ? ORDER BY d.post_id""", (version,)).fetchall()
        with open(out / "dataset_ids.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["post_id", "split", "operateur", "sentiment", "label_source", "score", "date"])
            w.writerows(r[:7] for r in rows)
        daily, totals = {}, {}
        topics = {}
        for _, _, op, sent, _, _, day, text in rows:
            op = op or "inconnu"
            if not sent:
                continue
            daily.setdefault((day or "inconnue", op), {"negatif": 0, "neutre": 0, "positif": 0})[sent] += 1
            totals.setdefault(op, {"negatif": 0, "neutre": 0, "positif": 0})[sent] += 1
            for s in sujets_de(text):
                topics.setdefault((day or "inconnue", op, s), {"negatif": 0, "neutre": 0, "positif": 0})[sent] += 1
        with open(out / "stats_sujets.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "operateur", "sujet", "negatif", "neutre", "positif", "total"])
            for (day, op, s), c in sorted(topics.items()):
                w.writerow([day, op, s, c["negatif"], c["neutre"], c["positif"], sum(c.values())])
        with open(out / "stats_par_jour.csv", "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["date", "operateur", "negatif", "neutre", "positif", "total"])
            for (day, op), c in sorted(daily.items()):
                w.writerow([day, op, c["negatif"], c["neutre"], c["positif"], sum(c.values())])
        summary = {"version": version, "generated_at": now(), "total": len(rows), "operateurs": {}}
        for op, c in sorted(totals.items()):
            n = sum(c.values())
            summary["operateurs"][op] = {**c, "total": n,
                                         "pct_negatif": round(100 * c["negatif"] / n, 1),
                                         "pct_positif": round(100 * c["positif"] / n, 1)}
        (out / "stats_resume.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        site = root / "docs" / "data"
        if site.parent.exists():
            site.mkdir(exist_ok=True)
            for name in ("stats_resume.json", "stats_par_jour.csv", "stats_sujets.csv"):
                shutil.copy2(out / name, site / name)
        log.info("Export public : %d lignes (version %s) dans %s", len(rows), version, out)
        return len(rows)
    finally:
        db.close()


def maybe_train(root, state, total, args, log):
    new = total - state.get("last_train_examples", 0)
    log.info("Fine-tuning : %d nouveaux exemples depuis le dernier entrainement (seuil %d)", new, args.train_threshold)
    if args.no_train or new < args.train_threshold:
        return
    train = root / "train.py"
    if not train.exists():
        log.warning("Seuil atteint mais train.py n'existe pas encore : entrainement saute.")
        return
    run_step("fine-tuning", ["train.py"], root, log, args.train_timeout_h)
    state["last_train_examples"] = total
    state["last_train_at"] = now()


def publish(root, log):
    """Commit + push des stats du site (docs/). Jamais de texte de post."""
    if not (root / ".git").exists():
        log.warning("Pas de depot git : publication ignoree.")
        return
    git = lambda *a: subprocess.run(["git", *a], cwd=root, capture_output=True, text=True)
    git("add", "docs")
    if git("diff", "--cached", "--quiet").returncode == 0:
        log.info("Site : aucune nouvelle statistique a publier.")
        return
    git("commit", "-m", f"Mise a jour automatique des stats {now()}")
    r = git("push")
    log.info("Site publie." if r.returncode == 0 else f"Echec du push : {r.stderr.strip()}")


def has_new_csv(root):
    return any((root / "collectes").glob("*.csv"))


def main():
    p = argparse.ArgumentParser(description="Pipeline automatique telecom-nlp")
    p.add_argument("--root", type=Path, default=ROOT)
    p.add_argument("--force", action="store_true", help="Relancer meme sans nouveau CSV.")
    p.add_argument("--device", choices=["cpu", "cuda"], default="cuda")
    p.add_argument("--no-publish", action="store_true", help="Ne pas pousser le site sur GitHub.")
    p.add_argument("--no-train", action="store_true")
    p.add_argument("--train-threshold", type=int, default=1000)
    p.add_argument("--step-timeout-h", type=float, default=6)
    p.add_argument("--train-timeout-h", type=float, default=12)
    args = p.parse_args()
    root = args.root.resolve()
    log = setup_log(root)
    state_path = root / "data" / "run_state.json"

    with Lock(root / "data" / "run_pipeline.lock"):
        state = load_state(state_path)
        if not args.force and not has_new_csv(root):
            log.info("Aucun nouveau CSV dans collectes/ : rien a faire.")
            return 0
        try:
            for name, cmd in STEPS:
                cmd = [c.format(device=args.device) for c in cmd]
                run_step(name, cmd, root, log, args.step_timeout_h)
            total = export_public(root, log)
            if not args.no_publish:
                publish(root, log)
            maybe_train(root, state, total, args, log)
            state["runs"] = state.get("runs", 0) + 1
            state["last_success_at"] = now()
            state["last_total"] = total
            log.info("Pipeline OK.")
            return 0
        except Exception as e:  # noqa: BLE001
            state["last_error"] = f"{now()} {e}"
            log.error("Pipeline en echec : %s", e)
            return 1
        finally:
            state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
