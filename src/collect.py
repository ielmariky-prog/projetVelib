import os
import datetime as dt
import requests
import pandas as pd
from huggingface_hub import HfApi

STATUS_URL = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json"
REPO_ID = "iey/velib-raw"

def flatten_bike_types(types_list):
    """[{'mechanical': 3}, {'ebike': 1}] -> {'mechanical': 3, 'ebike': 1}"""
    out = {}
    for d in types_list or []:
        out.update(d)
    return out

def main():
    now = dt.datetime.now(dt.timezone.utc)

    resp = requests.get(STATUS_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()

    stations = payload["data"]["stations"]
    if not stations:
        raise RuntimeError("Flux vide — abandon plutôt qu'écrire un fichier inutile")

    df = pd.DataFrame(stations)

    if "num_bikes_available_types" in df.columns:
        types = df["num_bikes_available_types"].apply(flatten_bike_types).apply(pd.Series)
        df = pd.concat([df.drop(columns=["num_bikes_available_types"]), types], axis=1)

    df["ingested_at"] = now
    df["feed_last_updated"] = payload.get("lastUpdatedOther") or payload.get("last_updated")

    local = "/tmp/status.parquet"
    df.to_parquet(local, index=False)

    path_in_repo = f"raw/status/date={now:%Y-%m-%d}/{now:%Y%m%dT%H%M%SZ}.parquet"
    HfApi().upload_file(
        path_or_fileobj=local,
        path_in_repo=path_in_repo,
        repo_id=REPO_ID,
        repo_type="dataset",
        token=os.environ["HF_TOKEN"],
    )
    print(f"OK {len(df)} stations -> {path_in_repo}")
if __name__ == "__main__":
    main()
