import streamlit as st, pandas as pd

st.title("展示会リスト Viewer")

RAW_URL = "https://raw.githubusercontent.com/<OWNER>/<REPO>/main/merged_list.csv"
df = pd.read_csv(RAW_URL)

st.sidebar.header("フィルター")
for col in df.columns:
    opts = st.sidebar.multiselect(col, df[col].unique())
    if opts:
        df = df[df[col].isin(opts)]

st.write(f"レコード数: {len(df)}")
st.dataframe(df)
st.download_button("CSV ダウンロード", df.to_csv(index=False), "list.csv")
