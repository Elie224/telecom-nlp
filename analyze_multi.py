"""Analyse multi-domaines (LLM, voitures, produits, personnalites, services, sport_jeux).

Lit collectes_multi/<domaine>/*.csv (meme format que collectes/), applique le meme
modele de sentiment que analyze_posts.py et ecrit UNIQUEMENT des statistiques
agregees (aucun texte de post) dans docs/data/multi/<domaine>/ :
  stats_par_jour.csv, stats_sujets.csv, stats_resume.json
et l'index docs/data/multi/domaines.json.

Les predictions sont mises en cache dans data/multi_predictions.sqlite3
(post_id + domaine + empreinte du texte) : seuls les nouveaux posts sont analyses.
Aucun fichier source n'est modifie ni supprime. Independant de la base telecom.

Usage : python analyze_multi.py [--device cpu|cuda] [--offline] [--root DOSSIER]
"""
import argparse
import csv
import hashlib
import json
import logging
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

from analyze_posts import MODEL_ID, PREPROCESS_VERSION, load_predictor, preprocess

DOMAINES = {
    "llm": "Intelligences artificielles (LLM)",
    "voitures": "Marques de voitures",
    "produits": "Produits et marques tech",
    "personnalites": "Personnalités",
    "services": "Services du quotidien",
    "sport_jeux": "Sport et jeux vidéo",
}
THEMES = {
    "prix": r"prix|tarif|cher|euros?|€|factur|augment|promo|abonnement|gratuit|payant|rembours",
    "qualite": r"bug|panne|nul|lent|qualit|marche pas|fonctionne pas|plant|erreur|d[ée]faut|g[ée]nial|excellent|top|incroyable|performant",
    "service_client": r"service client|sav|conseiller|hotline|r[ée]clamation|support|livraison|retard|attente",
    "polemique": r"scandale|pol[ée]mique|honte|boycott|arnaque|mensonge|clash",
}
THEMES_RE = {k: re.compile(v, re.I) for k, v in THEMES.items()}
SENTS = ("negatif", "neutre", "positif")


def themes_de(text):
    return [k for k, rx in THEMES_RE.items() if rx.search(text or "")] or ["autre"]


def read_domain(folder):
    posts = {}
    for path in sorted(folder.glob("*.csv")):
        with path.open(encoding="utf-8-sig", newline="") as f:
            for row in csv.DictReader(f):
                pid = (row.get("post_id") or "").strip()
                text = (row.get("text") or "").strip()
                lang = (row.get("lang") or "fr").strip().lower()
                if not pid or not text or lang not in ("fr", ""):
                    continue
                subjects = [s.strip() for s in (row.get("operators") or "").split("|") if s.strip()]
                posts[pid] = {"text": text, "date": (row.get("created_at") or "")[:10] or "inconnue",
                              "subjects": subjects or ["inconnu"]}
    return posts


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path(__file__).resolve().parent)
    p.add_argument("--device", choices=["cpu", "cuda"], default="cpu")
    p.add_argument("--offline", action="store_true")
    p.add_argument("--revision", default="main")
    args = p.parse_args()
    root = args.root.resolve()
    for name in ("logs", "data"):
        (root / name).mkdir(exist_ok=True)
    log = logging.getLogger("analyze_multi")
    log.setLevel(logging.INFO)
    for h in (logging.FileHandler(root / "logs" / "analyze_multi.log", encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        h.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        log.addHandler(h)

    src = root / "collectes_multi"
    out_root = root / "docs" / "data" / "multi"
    out_root.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(root / "data" / "multi_predictions.sqlite3")
    db.execute("""CREATE TABLE IF NOT EXISTS predictions (domaine TEXT, post_id TEXT, text_sha256 TEXT,
                  model_id TEXT, preprocessing_version TEXT, sentiment TEXT,
                  PRIMARY KEY (domaine, post_id, text_sha256, model_id, preprocessing_version))""")
    predict = None
    index = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "domaines": {}}
    try:
        for dom, label in DOMAINES.items():
            folder = src / dom
            posts = read_domain(folder) if folder.is_dir() else {}
            index["domaines"][dom] = {"label": label, "total": 0, "disponible": False}
            if not posts:
                log.info("%s : aucune donnee.", dom)
                continue
            sha = {pid: hashlib.sha256(v["text"].encode("utf-8")).hexdigest() for pid, v in posts.items()}
            known = {(r[0], r[1]): r[2] for r in db.execute(
                "SELECT post_id, text_sha256, sentiment FROM predictions WHERE domaine=? AND model_id=? AND preprocessing_version=?",
                (dom, MODEL_ID, PREPROCESS_VERSION))}
            todo = [pid for pid in posts if (pid, sha[pid]) not in known]
            if todo:
                if predict is None:
                    log.info("Chargement du modele %s", MODEL_ID)
                    predict, _, device = load_predictor(root, args.offline, args.revision, args.device)
                    log.info("Appareil : %s", device)
                todo.sort(key=lambda i: len(posts[i]["text"]))
                for start in range(0, len(todo), 256):
                    chunk = todo[start:start + 256]
                    res = predict([preprocess(posts[i]["text"]) for i in chunk])
                    vals = [(dom, pid, sha[pid], MODEL_ID, PREPROCESS_VERSION, max(s, key=s.get)) for pid, s in zip(chunk, res)]
                    with db:
                        db.executemany("INSERT OR REPLACE INTO predictions VALUES (?,?,?,?,?,?)", vals)
                    known.update({(v[1], v[2]): v[5] for v in vals})
            daily, topics, totals = {}, {}, {}
            for pid, v in posts.items():
                s = known[(pid, sha[pid])]
                th = themes_de(v["text"])
                for subj in v["subjects"]:
                    daily.setdefault((v["date"], subj), dict.fromkeys(SENTS, 0))[s] += 1
                    totals.setdefault(subj, dict.fromkeys(SENTS, 0))[s] += 1
                    for t in th:
                        topics.setdefault((v["date"], subj, t), dict.fromkeys(SENTS, 0))[s] += 1
            out = out_root / dom
            out.mkdir(exist_ok=True)
            with open(out / "stats_par_jour.csv", "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["date", "operateur", *SENTS, "total"])
                for (d, o), c in sorted(daily.items()):
                    w.writerow([d, o, *(c[k] for k in SENTS), sum(c.values())])
            with open(out / "stats_sujets.csv", "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["date", "operateur", "sujet", *SENTS, "total"])
                for (d, o, t), c in sorted(topics.items()):
                    w.writerow([d, o, t, *(c[k] for k in SENTS), sum(c.values())])
            resume = {"domaine": dom, "label": label, "generated_at": index["generated_at"], "total": len(posts),
                      "sujets": {o: {**c, "total": sum(c.values())} for o, c in sorted(totals.items())}}
            (out / "stats_resume.json").write_text(json.dumps(resume, ensure_ascii=False, indent=2), encoding="utf-8")
            index["domaines"][dom].update(total=len(posts), disponible=True)
            log.info("%s : %d posts (%d nouveaux analyses).", dom, len(posts), len(todo))
        (out_root / "domaines.json").write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
