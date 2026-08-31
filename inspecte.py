#!/usr/bin/env python3
"""
Inspecte des fichiers de collecte Vélib' : combien, quel format, quelles colonnes,
et est-ce compatible avec audit_velib.py.

Usage :
    python inspecte.py                        # cherche dans le dossier courant
    python inspecte.py mon/dossier
    python inspecte.py "s3://bucket/raw/status/**/*.parquet"

Ne modifie rien. Lit au plus quelques fichiers.
"""
from __future__ import annotations
import os, sys, pathlib, collections
import duckdb

EXT_LECTEUR = {
    ".parquet": "read_parquet('{g}', union_by_name = true)",
    ".json":    "read_json_auto('{g}')",
    ".gz":      "read_json_auto('{g}')",
    ".csv":     "read_csv_auto('{g}')",
    ".ndjson":  "read_json_auto('{g}')",
}

# ce dont audit_velib.py a besoin
REQUIS = ["station_id", "fetched_at", "last_reported", "num_bikes_available",
          "num_docks_available", "is_renting", "is_installed"]
SOUHAITES = ["mechanical", "ebike", "stationCode"]


def titre(t):
    print(f"\n\033[1m--- {t} ---\033[0m")


def trouver(chemin: str):
    """Retourne une liste de globs exploitables, groupés par extension."""
    if chemin.startswith("s3://") or "*" in chemin:
        ext = pathlib.Path(chemin.replace("*", "x")).suffix
        return {ext: chemin}

    p = pathlib.Path(chemin)
    if p.is_file():
        return {p.suffix: str(p)}

    if not p.is_dir():
        sys.exit(f"Chemin introuvable : {chemin}")

    fichiers = [f for f in p.rglob("*") if f.is_file() and f.suffix in EXT_LECTEUR]
    if not fichiers:
        print(f"Aucun fichier .parquet / .json / .csv trouvé sous {p.resolve()}")
        print("\nDossiers présents ici :")
        for d in sorted(x for x in p.iterdir() if x.is_dir())[:20]:
            print("   ", d.name + "/")
        sys.exit(1)

    # on groupe par (dossier de 1er niveau, extension) pour distinguer status / information
    groupes = collections.defaultdict(list)
    for f in fichiers:
        rel = f.relative_to(p)
        racine = rel.parts[0] if len(rel.parts) > 1 else "."
        # on descend jusqu'au dossier qui contient une partition date=
        cle = racine
        for part in rel.parts:
            if part.startswith("date="):
                break
            if part != rel.parts[-1]:
                cle = "/".join(rel.parts[:rel.parts.index(part) + 1])
        groupes[(cle, f.suffix)].append(f)

    resultat = {}
    for (cle, ext), fs in groupes.items():
        nom = (cle if cle != "." else p.name) + ext
        resultat[nom] = (str(p / cle / "**" / f"*{ext}"), len(fs),
                                   sum(x.stat().st_size for x in fs))
    return resultat


def normaliser(con, src: str):
    """Si c'est du JSON GBFS brut ({data:{stations:[...]}}), aplatit en une ligne
    par station. Renvoie (src_utilisable, a_ete_aplati)."""
    try:
        cols = con.sql(f"DESCRIBE SELECT * FROM {src} LIMIT 1").df()
    except Exception:
        return src, False
    noms = list(cols.column_name)
    if noms == ["data"] or ("data" in noms and "station_id" not in noms):
        src2 = f"(SELECT unnest(data.stations, max_depth := 2) FROM {src})"
        try:
            con.sql(f"SELECT * FROM {src2} LIMIT 1")
            return src2, True
        except Exception:
            pass
    if "stations" in noms and "station_id" not in noms:
        src2 = f"(SELECT unnest(stations, max_depth := 2) FROM {src})"
        try:
            con.sql(f"SELECT * FROM {src2} LIMIT 1")
            return src2, True
        except Exception:
            pass
    return src, False


def apercu(con, src: str):
    """3 lignes, colonnes imbriquées résumées, largeur bornée."""
    cols = con.sql(f"DESCRIBE SELECT * FROM {src} LIMIT 1").df()
    select = []
    for _, c in cols.iterrows():
        t = str(c.column_type).upper()
        n = c.column_name
        if any(k in t for k in ("STRUCT", "MAP", "[]")):
            select.append(f'left(CAST("{n}" AS VARCHAR), 34) AS "{n}"')
        else:
            select.append(f'"{n}"')
    df = con.sql(f"SELECT {', '.join(select)} FROM {src} LIMIT 3").df()
    for c in df.columns:
        df[c] = df[c].astype(str).str.slice(0, 34)
    return df


def decrire(con, glob: str, ext: str, label: str):
    lecteur = EXT_LECTEUR.get(ext)
    if not lecteur:
        print(f"Format non géré : {ext}")
        return None
    src = lecteur.format(g=glob)

    titre(f"Contenu de {label}")
    src, aplati = normaliser(con, src)
    if aplati:
        print("JSON GBFS brut détecté : aplati automatiquement pour l'analyse.\n")
    try:
        cols = con.sql(f"DESCRIBE SELECT * FROM {src} LIMIT 1").df()
    except Exception as e:
        print(f"Lecture impossible : {str(e).splitlines()[0]}")
        return None

    print("Colonnes :")
    for _, c in cols.iterrows():
        print(f"   {c.column_name:<28} {c.column_type}")

    n = con.sql(f"SELECT count(*) FROM {src}").fetchone()[0]
    print(f"\nNombre de lignes : {n:,}".replace(",", " "))

    noms = list(cols.column_name)
    if "fetched_at" in noms:
        per = con.sql(f"""SELECT min(fetched_at), max(fetched_at),
                                 count(DISTINCT fetched_at) FROM {src}""").fetchone()
        print(f"Période          : {per[0]}  →  {per[1]}")
        print(f"Collectes        : {per[2]}")
    if "station_id" in noms:
        s = con.sql(f"SELECT count(DISTINCT station_id) FROM {src}").fetchone()[0]
        print(f"Stations         : {s}")

    print("\n3 premières lignes :")
    import pandas as pd
    with pd.option_context("display.width", 150, "display.max_columns", 40,
                           "display.max_colwidth", 34):
        print(apercu(con, src).to_string(index=False))
    return noms, cols, src, aplati


def verdict(noms, cols):
    titre("Compatibilité avec audit_velib.py")
    manquants = [c for c in REQUIS if c not in noms]
    absents_souhaites = [c for c in SOUHAITES if c not in noms]

    if not manquants:
        print("Toutes les colonnes indispensables sont présentes.")
    else:
        print("Colonnes indispensables manquantes :")
        for c in manquants:
            print(f"   - {c}")

    if absents_souhaites:
        print("\nColonnes optionnelles absentes :", ", ".join(absents_souhaites))

    # cas fréquent : la liste de dicts n'a pas été aplatie
    types = dict(zip(cols.column_name, cols.column_type))
    imbrique = [c for c, t in types.items()
                if any(k in str(t).upper() for k in ("STRUCT", "MAP", "[]"))]
    if imbrique:
        print("\nColonnes imbriquées détectées :", ", ".join(imbrique))
        if "num_bikes_available_types" in imbrique:
            print("   → C'est la liste [{mechanical: n}, {ebike: n}] de l'API.")
            print("     À aplatir en deux colonnes entières avant l'audit.")

    if "fetched_at" not in noms:
        print("\nAttention : pas de colonne `fetched_at` (ton horodatage de collecte).")
        print("   Si tu as un autre nom (ts, collected_at, timestamp...), renomme-le,")
        print("   c'est l'axe temporel de toute la suite.")

    return not manquants


def convertir(con, glob, ext, dest):
    """JSON GBFS brut -> parquet aplati, avec fetched_at repris du nom de fichier."""
    base = f"read_json_auto('{glob}', filename = true)"
    probe = list(con.sql(f"DESCRIBE SELECT * FROM {base} LIMIT 1").df().column_name)
    if "station_id" in probe:
        src = f"(SELECT * FROM {base})"
    elif "data" in probe:
        src = f"(SELECT filename, unnest(data.stations, max_depth := 2) FROM {base})"
    else:
        src = f"(SELECT filename, unnest(stations, max_depth := 2) FROM {base})"
    cols = list(con.sql(f"DESCRIBE SELECT * FROM {src} LIMIT 1").df().column_name)

    if "fetched_at" in cols:
        ts = "fetched_at"
    else:
        ts = "strptime(regexp_extract(filename, '(\\d{8}T\\d{6})Z', 1), '%Y%m%dT%H%M%S')"
        print("fetched_at reconstruit depuis le nom de fichier.")

    lr = ("to_timestamp(last_reported)" if "last_reported" in cols else "NULL")
    pathlib.Path(dest).mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
            SELECT station_id, stationCode,
                   num_bikes_available, num_docks_available,
                   COALESCE(list_extract(list_filter(
                       num_bikes_available_types, x -> x.mechanical IS NOT NULL),1).mechanical,0) AS mechanical,
                   COALESCE(list_extract(list_filter(
                       num_bikes_available_types, x -> x.ebike IS NOT NULL),1).ebike,0) AS ebike,
                   is_installed, is_renting, is_returning,
                   {lr} AS last_reported,
                   {ts} AS fetched_at,
                   CAST({ts} AS DATE) AS date
            FROM {src}
        ) TO '{dest}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE);
    """)
    n = con.sql(f"SELECT count(*) FROM read_parquet('{dest}/**/*.parquet')").fetchone()[0]
    print(f"{n:,} lignes écrites dans {dest}/".replace(",", " "))
    print(f'\nLance maintenant :\n   python audit_velib.py --status "{dest}/**/*.parquet"')


def main():
    chemin = sys.argv[1] if len(sys.argv) > 1 else "."
    dest = None
    if "--convertir" in sys.argv:
        dest = sys.argv[sys.argv.index("--convertir") + 1]
    con = duckdb.connect()
    if chemin.startswith("s3://"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
        k, s, e = (os.getenv("R2_ACCESS_KEY_ID"), os.getenv("R2_SECRET_ACCESS_KEY"),
                   os.getenv("R2_ENDPOINT"))
        if not all([k, s, e]):
            sys.exit("Identifiants R2 manquants dans l'environnement.")
        con.execute(f"""CREATE SECRET r2 (TYPE S3, KEY_ID '{k}', SECRET '{s}',
                        ENDPOINT '{e}', URL_STYLE 'path', REGION 'auto');""")

    trouves = trouver(chemin)

    if dest:
        titre("Conversion")
        for label, v in trouves.items():
            g = v[0] if isinstance(v, tuple) else v
            e = pathlib.Path(g.replace("*", "x")).suffix
            if e in (".json", ".gz", ".ndjson"):
                convertir(con, g, e, dest)
                return

    titre("Fichiers trouvés")
    for label, v in trouves.items():
        if isinstance(v, tuple):
            g, nb, taille = v
            print(f"   {label:<24} {nb:>5} fichiers   {taille/1e6:>8.1f} Mo")
        else:
            print(f"   {label:<24} {v}")

    ok_status = False
    for label, v in trouves.items():
        glob = v[0] if isinstance(v, tuple) else v
        ext = pathlib.Path(glob.replace("*", "x")).suffix
        res = decrire(con, glob, ext, label)
        if res and "num_bikes_available" in res[0]:
            ok_status = verdict(res[0], res[1])
            chemin_status, source_aplatie = glob, res[3]

    titre("Prochaine étape")
    if ok_status:
        print("Tes fichiers sont lisibles et complets. Lance maintenant :\n")
        if source_aplatie:
            print("   Tes données sont en JSON GBFS brut. Convertis-les d'abord :\n")
            print(f'   python inspecte.py "{chemin_status}" --convertir sortie/')
            print("\n   puis :\n")
            print('   python audit_velib.py --status "sortie/**/*.parquet"')
        else:
            print(f'   python audit_velib.py --status "{chemin_status}"')
        print("\n(ajoute --info \"...information/**/*.parquet\" si tu as le référentiel)")
    else:
        print("Corrige d'abord le schéma ci-dessus, puis relance ce script.")


if __name__ == "__main__":
    main()