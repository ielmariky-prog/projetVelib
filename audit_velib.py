#!/usr/bin/env python3
"""
Audit de l'historique Vélib' collecté.

Usage
-----
  # sur des fichiers locaux
  python audit_velib.py --status "fake/raw/status/**/*.parquet" \
                        --info   "fake/raw/information/**/*.parquet"

  # sur Cloudflare R2 / S3 (identifiants dans l'environnement)
  export R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... \
         R2_ENDPOINT=xxxx.r2.cloudflarestorage.com
  python audit_velib.py --status "s3://velib/raw/status/**/*.parquet" \
                        --info   "s3://velib/raw/information/**/*.parquet"

Écrit un rapport Markdown (--out, défaut audit_report.md) et affiche
l'essentiel sur la sortie standard.

Hypothèses de schéma (couche brute) :
  status : station_id, fetched_at, last_reported, num_bikes_available,
           mechanical, ebike, num_docks_available, is_renting, is_installed
  info   : station_id, capacity, name, lat, lon, fetched_at
Le pas de collecte visé est de 15 minutes (--step-min pour le changer).
"""
from __future__ import annotations
import argparse, os, sys, textwrap
import duckdb

STEP = 15      # minutes, écrasé par --step-min
HAS_ICU = True  # renseigné par connect()


# --------------------------------------------------------------------------- #
# infrastructure
# --------------------------------------------------------------------------- #
def config_acces(con, chemin: str):
    """Configure DuckDB selon la destination : local, hf:// ou s3://."""
    if chemin.startswith("hf://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        tok = os.getenv("HF_TOKEN")
        if not tok:
            sys.exit("Variable HF_TOKEN manquante.\n"
                     "   export HF_TOKEN=hf_xxxxxxxxxxxx")
        con.execute(f"CREATE SECRET hf (TYPE HUGGINGFACE, TOKEN '{tok}');")
    elif chemin.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        k, sec, e = (os.getenv("R2_ACCESS_KEY_ID"), os.getenv("R2_SECRET_ACCESS_KEY"),
                     os.getenv("R2_ENDPOINT"))
        if not all([k, sec, e]):
            sys.exit("Identifiants R2 manquants : R2_ACCESS_KEY_ID, "
                     "R2_SECRET_ACCESS_KEY, R2_ENDPOINT")
        con.execute(f"""CREATE SECRET r2 (TYPE S3, KEY_ID '{k}', SECRET '{sec}',
                        ENDPOINT '{e}', URL_STYLE 'path', REGION 'auto');""")


def connect(status_glob: str, info_glob: str | None) -> duckdb.DuckDBPyConnection:
    global HAS_ICU
    con = duckdb.connect()
    try:
        con.execute("INSTALL icu; LOAD icu;")
        HAS_ICU = True
    except Exception:
        HAS_ICU = False
        print("Note : extension ICU indisponible, les heures locales seront approximées "
              "par UTC+2 (pas de gestion du changement d'heure).", file=sys.stderr)
    for chemin in (status_glob, info_glob or ""):
        if chemin.startswith(("hf://", "s3://")):
            config_acces(con, chemin)
            break

    con.execute(f"""
        CREATE VIEW raw_status AS
        SELECT * FROM read_parquet('{status_glob}', union_by_name = true);
    """)
    if info_glob:
        con.execute(f"""
            CREATE VIEW raw_info AS
            SELECT * FROM read_parquet('{info_glob}', union_by_name = true);
            -- dernier snapshot connu de chaque station
            CREATE VIEW ref_stations AS
            SELECT * FROM raw_info
            QUALIFY row_number() OVER (PARTITION BY station_id ORDER BY fetched_at DESC) = 1;
        """)
    return con


class Report:
    """Accumule le Markdown et l'affiche au fil de l'eau."""
    def __init__(self):
        self.lines: list[str] = []

    def h(self, title: str):
        self.lines.append(f"\n## {title}\n")
        print(f"\n\033[1m=== {title} ===\033[0m")

    def p(self, text: str):
        self.lines.append(text + "\n")
        print(textwrap.fill(text.replace("**", ""), 88))

    def table(self, df):
        self.lines.append(df.to_markdown(index=False) + "\n")
        print(df.to_string(index=False))

    def kv(self, label: str, value, verdict: str = ""):
        line = f"- **{label}** : {value}" + (f" — {verdict}" if verdict else "")
        self.lines.append(line)
        print(f"  {label:<42} {value}" + (f"   [{verdict}]" if verdict else ""))

    def save(self, path: str):
        with open(path, "w") as f:
            f.write("# Audit de l'historique Vélib'\n" + "\n".join(self.lines))


def flag(ok: bool, warn: bool = False) -> str:
    return "OK" if ok else ("à surveiller" if warn else "PROBLÈME")


# --------------------------------------------------------------------------- #
# sections d'audit
# --------------------------------------------------------------------------- #
def section_volume(con, r):
    r.h("1. Volume et couverture")
    q = con.sql("""
        SELECT count(*)                       AS n_lignes,
               count(DISTINCT fetched_at)     AS n_runs,
               count(DISTINCT station_id)     AS n_stations,
               min(fetched_at)                AS debut,
               max(fetched_at)                AS fin,
               epoch(max(fetched_at) - min(fetched_at)) / 86400 AS jours
        FROM raw_status
    """).df().iloc[0]
    r.kv("Lignes", f"{q.n_lignes:,}".replace(",", " "))
    r.kv("Collectes distinctes", q.n_runs)
    r.kv("Stations vues au moins une fois", q.n_stations)
    r.kv("Période couverte", f"{q.debut} → {q.fin} ({q.jours:.1f} jours)")
    attendu = q.jours * 24 * 60 / STEP
    taux = q.n_runs / attendu if attendu else 0
    r.kv("Taux de collecte réel", f"{taux:.1%} des {attendu:.0f} runs attendus",
         flag(taux > 0.97, taux > 0.90))
    return q


def section_cadence(con, r):
    r.h("2. Cadence du cron")
    r.p("Le cron GitHub Actions est *best effort* : il se décale et saute des exécutions. "
        "C'est cette section qui justifie de rééchantillonner sur une grille régulière "
        "plutôt que de supposer que la ligne précédente est à -15 min.")
    df = con.sql(f"""
        WITH runs AS (SELECT DISTINCT fetched_at FROM raw_status),
             gaps AS (SELECT fetched_at,
                             epoch(fetched_at - lag(fetched_at) OVER (ORDER BY fetched_at)) AS ecart_s
                      FROM runs)
        SELECT date_trunc('day', fetched_at)::DATE           AS jour,
               count(*)                                      AS runs,
               round(median(ecart_s) / 60, 1)                AS ecart_median_min,
               round(max(ecart_s) / 60, 1)                   AS pire_trou_min,
               count(*) FILTER (WHERE ecart_s > {STEP * 60 * 1.5}) AS runs_manques
        FROM gaps GROUP BY 1 ORDER BY 1
    """).df()
    r.table(df)
    pire = df.pire_trou_min.max()
    r.kv("Pire interruption", f"{pire:.0f} min", flag(pire < 45, pire < 180))
    r.kv("Runs manqués (total)", int(df.runs_manques.sum()))
    return df


def section_referentiel(con, r):
    r.h("3. Stabilité du référentiel")
    r.p("Des stations ouvrent, ferment pour travaux, changent de capacité. Une variation "
        "de quelques unités est normale ; une chute brutale signale une collecte tronquée.")
    df = con.sql("""
        SELECT date_trunc('day', fetched_at)::DATE AS jour,
               count(DISTINCT station_id)          AS n_stations
        FROM raw_status GROUP BY 1 ORDER BY 1
    """).df()
    r.table(df)
    ecart = int(df.n_stations.max() - df.n_stations.min())
    r.kv("Amplitude jour le plus riche / le plus pauvre", f"{ecart} stations",
         flag(ecart < 15, ecart < 100))

    dispar = con.sql("""
        WITH bornes AS (
            SELECT station_id, min(fetched_at) AS vue_le_premier,
                                max(fetched_at) AS vue_le_dernier
            FROM raw_status GROUP BY 1),
        globale AS (SELECT max(fetched_at) AS fin FROM raw_status)
        SELECT count(*) FROM bornes, globale
        WHERE vue_le_dernier < fin - INTERVAL 12 HOUR
    """).fetchone()[0]
    r.kv("Stations absentes du flux depuis >12 h", dispar,
         flag(dispar == 0, dispar < 30))


def section_fraicheur(con, r):
    r.h("4. Fraîcheur des capteurs")
    r.p("`fetched_at - last_reported` est l'écart entre l'instant où tu interroges l'API et "
        "l'instant que la station déclare. C'est cette distribution qui fixe le seuil de "
        "« capteur mort » — au lieu de le choisir arbitrairement.")
    q = con.sql("""
        SELECT round(quantile_cont(epoch(fetched_at - last_reported), 0.50)) AS p50_s,
               round(quantile_cont(epoch(fetched_at - last_reported), 0.90) / 60) AS p90_min,
               round(quantile_cont(epoch(fetched_at - last_reported), 0.99) / 3600, 1) AS p99_h,
               round(max(epoch(fetched_at - last_reported)) / 3600, 1) AS max_h,
               round(100.0 * count(*) FILTER (WHERE fetched_at - last_reported > INTERVAL 6 HOUR)
                     / count(*), 2) AS pct_sup_6h,
               round(100.0 * count(*) FILTER (WHERE fetched_at - last_reported > INTERVAL 24 HOUR)
                     / count(*), 2) AS pct_sup_24h
        FROM raw_status WHERE last_reported IS NOT NULL
    """).df().iloc[0]
    r.kv("Médiane", f"{q.p50_s:.0f} s")
    r.kv("p90", f"{q.p90_min:.0f} min")
    r.kv("p99", f"{q.p99_h:.1f} h")
    r.kv("Maximum", f"{q.max_h:.1f} h")
    r.kv("Observations avec fraîcheur > 6 h", f"{q.pct_sup_6h:.2f} %")
    r.kv("Observations avec fraîcheur > 24 h", f"{q.pct_sup_24h:.2f} %",
         flag(q.pct_sup_24h < 1, q.pct_sup_24h < 5))


def section_coherence(con, r, has_ref: bool):
    r.h("5. Cohérence des valeurs")
    join = "LEFT JOIN ref_stations USING (station_id)" if has_ref else ""
    cap = ("count(*) FILTER (WHERE num_bikes_available + num_docks_available > capacity)"
           if has_ref else "NULL")
    q = con.sql(f"""
        SELECT count(*) AS n,
               count(*) FILTER (WHERE num_bikes_available IS NULL) AS nulls_bikes,
               count(*) FILTER (WHERE mechanical + ebike <> num_bikes_available) AS types_incoherents,
               count(*) FILTER (WHERE num_bikes_available < 0
                                   OR num_docks_available < 0)     AS negatifs,
               {cap} AS depasse_capacite,
               count(*) FILTER (WHERE NOT is_renting)               AS hors_service,
               count(*) FILTER (WHERE NOT is_installed)             AS non_installees
        FROM raw_status {join}
    """).df().iloc[0]
    n = q.n
    def pct(x):
        return "n/a" if x is None else f"{x:,}".replace(",", " ") + f"  ({100 * x / n:.2f} %)"
    r.kv("num_bikes_available NULL", pct(q.nulls_bikes), flag(q.nulls_bikes / n < 0.01))
    r.kv("mechanical + ebike ≠ total", pct(q.types_incoherents),
         flag(q.types_incoherents / n < 0.005, q.types_incoherents / n < 0.05))
    r.kv("Valeurs négatives", pct(q.negatifs), flag(q.negatifs == 0))
    if has_ref:
        r.kv("vélos + bornes > capacité", pct(q.depasse_capacite),
             flag(q.depasse_capacite / n < 0.01, q.depasse_capacite / n < 0.05))
    r.kv("is_renting = false", pct(q.hors_service))
    r.kv("is_installed = false", pct(q.non_installees))
    r.p("Ces pourcentages sont à noter : « 2,1 % des observations avaient une somme "
        "mécaniques + électriques incohérente avec le total » est une phrase d'entretien, "
        "« j'ai nettoyé les données » n'en est pas une.")


def section_figes(con, r):
    r.h("6. Capteurs figés")
    r.p("Détection par *gaps and islands* : une station dont le compteur ne bouge pas "
        "pendant plusieurs heures est presque sûrement hors service. On compare ce signal "
        "à celui de la fraîcheur — les deux ne se recouvrent pas exactement, et l'écart "
        "est instructif.")
    df = con.sql(f"""
        WITH ordonne AS (
            SELECT station_id, fetched_at, num_bikes_available AS b,
                   lag(num_bikes_available) OVER (PARTITION BY station_id ORDER BY fetched_at) AS b_prec,
                   fetched_at - last_reported AS fraicheur
            FROM raw_status WHERE num_bikes_available IS NOT NULL),
        marque AS (
            SELECT *, sum(CASE WHEN b IS DISTINCT FROM b_prec THEN 1 ELSE 0 END)
                        OVER (PARTITION BY station_id ORDER BY fetched_at) AS plage
            FROM ordonne),
        plages AS (
            SELECT station_id, plage, count(*) AS n_obs,
                   min(fetched_at) AS debut, max(fetched_at) AS fin,
                   epoch(max(fetched_at) - min(fetched_at)) / 3600 AS duree_h,
                   max(fraicheur) AS fraicheur_max
            FROM marque GROUP BY 1, 2)
        SELECT seuil_h AS seuil_heures,
               count(*) AS n_plages,
               count(DISTINCT station_id) AS n_stations,
               count(*) FILTER (WHERE fraicheur_max > INTERVAL 6 HOUR) AS dont_fraicheur_confirme
        FROM plages, (SELECT unnest([3, 6, 12, 24]) AS seuil_h)
        WHERE duree_h >= seuil_h
        GROUP BY 1 ORDER BY 1
    """).df()
    r.table(df)
    if len(df):
        row = df[df.seuil_heures == 6]
        if len(row):
            row = row.iloc[0]
            accord = row.dont_fraicheur_confirme / row.n_plages if row.n_plages else 0
            r.kv("Seuil 6 h : stations concernées", int(row.n_stations))
            r.kv("Confirmées aussi par la fraîcheur", f"{accord:.0%}",
                 "les deux signaux se recoupent" if accord > 0.7
                 else "signaux divergents, à creuser")


def section_grille(con, r):
    r.h("7. Grille régulière et trous")
    r.p(f"On projette les observations sur une grille de {STEP} min par station. "
        "Le taux de remplissage donne la proportion de cases réellement observées : "
        "c'est le vrai coût des runs manqués, station par station.")
    con.execute(f"""
        CREATE OR REPLACE VIEW grille AS
        WITH bornes AS (
            SELECT date_trunc('hour', min(fetched_at)) AS t0, max(fetched_at) AS t1
            FROM raw_status),
        pas AS (
            SELECT unnest(generate_series(t0, t1, INTERVAL {STEP} MINUTE)) AS ts FROM bornes),
        stations AS (SELECT DISTINCT station_id FROM raw_status),
        obs AS (
            SELECT station_id,
                   time_bucket(INTERVAL {STEP} MINUTE, fetched_at) AS ts,
                   num_bikes_available AS n_bikes
            FROM raw_status
            QUALIFY row_number() OVER (
                PARTITION BY station_id, time_bucket(INTERVAL {STEP} MINUTE, fetched_at)
                ORDER BY fetched_at DESC) = 1)
        SELECT s.station_id, p.ts, o.n_bikes, o.n_bikes IS NULL AS is_imputed
        FROM stations s CROSS JOIN pas p
        LEFT JOIN obs o ON o.station_id = s.station_id AND o.ts = p.ts;
    """)
    q = con.sql("""
        SELECT count(*) AS cases,
               round(100.0 * count(*) FILTER (WHERE NOT is_imputed) / count(*), 2) AS pct_remplies
        FROM grille
    """).df().iloc[0]
    r.kv("Cases de la grille", f"{q.cases:,}".replace(",", " "))
    r.kv("Cases réellement observées", f"{q.pct_remplies:.2f} %",
         flag(q.pct_remplies > 95, q.pct_remplies > 85))
    pires = con.sql("""
        SELECT station_id,
               round(100.0 * count(*) FILTER (WHERE is_imputed) / count(*), 1) AS pct_trous
        FROM grille GROUP BY 1 ORDER BY pct_trous DESC LIMIT 5
    """).df()
    r.p("**Stations les plus lacunaires :**")
    r.table(pires)


def section_baseline(con, r):
    r.h("8. Baseline naïve (bonus)")
    r.p("« Le nombre de vélos dans 60 min = le nombre actuel ». C'est l'étalon de tout le "
        "reste du projet, et il s'obtient en une requête. Le calculer maintenant, avant "
        "d'écrire la moindre ligne de LightGBM, est l'ordre dans lequel on travaille.")
    horizon = 60 // STEP
    local_expr = ("(ts AT TIME ZONE 'UTC') AT TIME ZONE 'Europe/Paris'" if HAS_ICU
                  else "ts + INTERVAL 2 HOUR")
    con.execute(f"""
        CREATE OR REPLACE VIEW cible AS
        SELECT station_id, ts, n_bikes, is_imputed,
               lead(n_bikes,    {horizon}) OVER w AS y,
               lead(is_imputed, {horizon}) OVER w AS y_imputed,
               extract('hour' FROM {local_expr}) AS h_paris
        FROM grille
        WINDOW w AS (PARTITION BY station_id ORDER BY ts);
    """)
    # La grille étant complète et régulière, lead(n) vaut exactement +60 min.
    # On n'évalue que sur les couples réellement observés aux deux bouts.
    con.execute("""
        CREATE OR REPLACE VIEW cible_eval AS
        SELECT * FROM cible WHERE NOT is_imputed AND NOT y_imputed AND y IS NOT NULL;
    """)
    mae = con.sql("SELECT round(avg(abs(n_bikes - y)), 3) FROM cible_eval").fetchone()[0]
    r.kv("MAE de la baseline (tous créneaux)", f"{mae} vélos")
    df = con.sql("""
        SELECT CASE WHEN h_paris BETWEEN 0 AND 5  THEN '00-06 nuit'
                    WHEN h_paris BETWEEN 6 AND 9  THEN '06-10 pointe matin'
                    WHEN h_paris BETWEEN 10 AND 15 THEN '10-16 journée'
                    WHEN h_paris BETWEEN 16 AND 19 THEN '16-20 pointe soir'
                    ELSE '20-24 soirée' END AS creneau,
               count(*) AS n,
               round(avg(abs(n_bikes - y)), 3) AS mae
        FROM cible_eval GROUP BY 1 ORDER BY 1
    """).df()
    r.table(df)
    r.p("L'écart entre le meilleur et le pire créneau indique où un modèle a une chance "
        "de gagner quelque chose. Si la baseline est déjà excellente la nuit, inutile "
        "d'espérer y briller.")


def section_conclusion(con, r):
    r.h("9. Ce que cet audit implique pour dbt")
    for point in [
        "Le cron étant irrégulier, **aucune feature ne doit reposer sur `LAG` brut** : "
        "il faut passer par la grille régulière construite en section 7.",
        "Conserver `is_imputed` jusque dans le mart : un trou imputé n'est pas une observation.",
        "Le seuil de capteur figé se choisit à partir de la section 6, pas au doigt mouillé.",
        "Les lignes `is_renting = false` et les plages figées doivent être **exclues de "
        "l'évaluation** mais **gardées dans les features** (une station HS voisine influence "
        "la demande).",
        "Tester dans dbt : unicité `(station_id, fetched_at)`, plage 0-100 sur `n_bikes`, "
        "absence de trou dans la grille, et `y` non renseignée sur les derniers créneaux.",
    ]:
        r.lines.append(f"- {point}")
        print("  • " + textwrap.fill(point.replace("**", ""), 84, subsequent_indent="    "))


# --------------------------------------------------------------------------- #
def main():
    global STEP
    ap = argparse.ArgumentParser(description="Audit de l'historique Vélib'")
    ap.add_argument("--status", required=True, help="glob parquet des statuts")
    ap.add_argument("--info", default=None, help="glob parquet du référentiel")
    ap.add_argument("--out", default="audit_report.md")
    ap.add_argument("--step-min", type=int, default=15)
    a = ap.parse_args()
    STEP = a.step_min

    con = connect(a.status, a.info)
    r = Report()
    section_volume(con, r)
    section_cadence(con, r)
    section_referentiel(con, r)
    section_fraicheur(con, r)
    section_coherence(con, r, has_ref=a.info is not None)
    section_figes(con, r)
    section_grille(con, r)
    section_baseline(con, r)
    section_conclusion(con, r)
    r.save(a.out)
    print(f"\nRapport écrit dans {a.out}")


if __name__ == "__main__":
    main()
