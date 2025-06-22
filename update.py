from notion_client import Client
import pandas as pd, os, requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DATABASE_ID   = "ここに Notion の Database ID"
notion = Client(auth=NOTION_TOKEN)

# --- fetch & clean 関数はあとで差し替え ---
def fetch_and_clean():
    # ダミー：空DataFrame返す
    return pd.DataFrame({"sample": [1]})

if __name__ == "__main__":
    df = fetch_and_clean()
    df.to_csv("merged_list.csv", index=False)
