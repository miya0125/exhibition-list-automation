import streamlit as st
import pandas as pd
import os
import requests
from datetime import datetime
import re
import tempfile
import zipfile
import io
import logging
from charset_normalizer import detect
from notion_client import Client
import json

# ページ設定
st.set_page_config(
    page_title="展示会リスト自動化システム（高機能版）",
    page_icon="📊",
    layout="wide"
)

# セッション状態の初期化
if 'merged_data' not in st.session_state:
    st.session_state.merged_data = pd.DataFrame()
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []
if 'processing_stats' not in st.session_state:
    st.session_state.processing_stats = {}

# 拡張版列名マッピング
COLUMN_RENAMES = {
    # 会社名関連
    '会社名': '会社名', '企業名': '会社名', '出展社名': '会社名',
    '出展社': '会社名', '法人名': '会社名', 'COMPANY': '会社名', 
    '社名': '会社名', 'Company': '会社名', 'company': '会社名',
    '出展企業名': '会社名', '出展者名': '会社名', 'Corporate Name': '会社名',
    '組織名': '会社名', 'Organization': '会社名', '団体名': '会社名',
    
    # 担当者関連
    '担当者名': '担当者', '氏名': '担当者', '名前': '担当者', 
    '担当': '担当者', '担当窓口': '担当者', '責任者': '担当者', 
    'ご担当者': '担当者', 'Contact Person': '担当者', 'Person': '担当者',
    '営業担当': '担当者', '代表者': '担当者', '担当者様': '担当者',
    'お名前': '担当者', 'Name': '担当者', '連絡担当者': '担当者',
    
    # メールアドレス関連
    'メールアドレス': 'メールアドレス', 'メール': 'メールアドレス',
    '担当者メール': 'メールアドレス', 'e-mail': 'メールアドレス',
    'Eメール': 'メールアドレス', 'Email': 'メールアドレス', 
    'E-Mail': 'メールアドレス', 'email': 'メールアドレス',
    'Mail': 'メールアドレス', 'mail': 'メールアドレス',
    'E-mailアドレス': 'メールアドレス', 'Emailアドレス': 'メールアドレス',
    'メアド': 'メールアドレス', 'Mail Address': 'メールアドレス',
    
    # 業界関連
    '業界名': '業界', '業界': '業界', '業種': '業界', 
    '分野': '業界', 'Industry': '業界', '事業分野': '業界',
    'カテゴリー': '業界', 'Category': '業界', '部門': '業界',
    '業態': '業界', 'Sector': '業界', '産業': '業界',
    
    # 展示会名関連
    '展示会名': '展示会名', '展覧会': '展示会名', '展示会': '展示会名', 
    'EXPO': '展示会名', 'イベント名': '展示会名', 'Event': '展示会名',
    'Exhibition': '展示会名', 'Show': '展示会名', 'Fair': '展示会名',
    '見本市': '展示会名', 'Trade Show': '展示会名', 'イベント': '展示会名',
    
    # 電話番号関連
    '電話番号': 'Tel', 'Tel': 'Tel', '電話': 'Tel', 'TEL': 'Tel', 'Phone': 'Tel', 
    'TEL_FAX': 'Tel', 'TELとFAX': 'Tel', 'tel': 'Tel', 'phone': 'Tel',
    'Telephone': 'Tel', 'Phone Number': 'Tel', '連絡先電話': 'Tel',
    'TEL番号': 'Tel', 'Tel番号': 'Tel', '℡': 'Tel',
    
    # ウェブサイト関連
    'ウェブサイト': 'Website', 'WEBサイト': 'Website', 'HP': 'Website', 
    'ホームページ': 'Website', 'URL': 'Website', 'Web': 'Website', 'web': 'Website',
    'Website URL': 'Website', 'ウェブ': 'Website', 'WEB': 'Website', 'サイト': 'Website',
    'Homepage': 'Website', 'WebサイトURL': 'Website', 'ウェブサイトURL': 'Website',
    
    # 郵便番号関連
    '郵便番号': 'YuubinBangou', '〒': 'YuubinBangou', 'ZIPコード': 'YuubinBangou', 
    'ZIP': 'YuubinBangou', 'Zip Code': 'YuubinBangou', 'Postal Code': 'YuubinBangou',
    '郵便': 'YuubinBangou', 'ゆうびん番号': 'YuubinBangou',
    
    # 住所関連
    '住所': 'Address', '所在地': 'Address', 'アドレス': 'Address', '本社': 'Address', 
    '本社所在地': 'Address', '本社住所': 'Address', '会社住所': 'Address',
    'Location': 'Address', '事業所': 'Address', '営業所': 'Address',
    '所在': 'Address', '番地': 'Address',
    
    # FAX関連
    'FAX番号': 'FAX', 'FAX': 'FAX', 'Fax': 'FAX', 'ファックス': 'FAX',
    'fax': 'FAX', 'ファクス': 'FAX', 'Facsimile': 'FAX',
    
    # 問い合わせ先関連
    '問い合わせ先': '問い合わせ先', '連絡先': '連絡先', 'お問い合わせ先': 'お問い合わせ先', 
    'Contact': '連絡先', 'お問合せ先': 'お問い合わせ先', '問合せ先': '問い合わせ先',
    'Contact Information': '連絡先', '連絡先情報': '連絡先',
    
    # その他
    '備考': '備考', 'メモ': '備考', 'Note': '備考', 'Memo': '備考', 'コメント': '備考',
    '部署': '部署', 'Department': '部署', '所属': '部署', 'Division': '部署',
    '役職': '役職', 'Title': '役職', 'Position': '役職', '肩書': '役職'
}

REQUIRED_COLUMNS = ["メールアドレス", "展示会名", "担当者", "業界", "Tel", "会社名"]
KEY_COLS = ["展示会名", "業界", "会社名"]

# データ抽出・正規化関数
def extract_email_from_text(text):
    """テキストからメールアドレスを抽出"""
    if pd.isna(text) or text == "":
        return ""
    
    text = str(text)
    patterns = [
        r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'([a-zA-Z0-9._%+-]+[@＠][a-zA-Z0-9.-]+[.．][a-zA-Z]{2,})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            email = match.group(1)
            email = email.replace('＠', '@').replace('．', '.')
            return email.lower()
    
    return ""

def extract_phone_from_text(text):
    """テキストから電話番号を抽出"""
    if pd.isna(text) or text == "":
        return ""
    
    text = str(text)
    text = text.translate(str.maketrans('０１２３４５６７８９－（）', '0123456789-()'))
    
    patterns = [
        r'(?:TEL|Tel|tel|電話|℡)[:：]?\s*([0-9\-\(\)\s]+)',
        r'(?:^|[\s])([0-9]{2,4}[\-][0-9]{2,4}[\-][0-9]{3,4})',
        r'(?:^|[\s])(\([0-9]{2,4}\)[0-9]{2,4}[\-]?[0-9]{3,4})',
        r'(?:^|[\s])([0-9]{10,11})',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            phone = match.group(1).strip()
            if len(re.sub(r'[^0-9]', '', phone)) >= 10:
                return normalize_phone(phone)
    
    return ""

def normalize_phone(phone_str):
    """電話番号を統一フォーマットに整理"""
    if pd.isna(phone_str) or phone_str == "":
        return phone_str
    
    phone_str = str(phone_str).strip()
    phone_str = phone_str.translate(str.maketrans('０１２３４５６７８９－（）', '0123456789-()'))
    phone_str = re.sub(r'[^\d\-+\(\)]', '', phone_str)
    
    digits_only = re.sub(r'[^\d]', '', phone_str)
    if len(digits_only) == 10 and digits_only[0] == '0':
        return f"{digits_only[:2]}-{digits_only[2:6]}-{digits_only[6:]}"
    elif len(digits_only) == 11 and digits_only[0] == '0':
        return f"{digits_only[:3]}-{digits_only[3:7]}-{digits_only[7:]}"
    
    return phone_str

def validate_email(email):
    """メールアドレスの妥当性を検証"""
    if pd.isna(email) or email == "":
        return email
    
    email = str(email).strip().lower()
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(pattern, email):
        return email
    return ""

def is_google_sheet_url(url):
    """Googleスプレッドシートかどうか判定"""
    patterns = [
        r"https://docs\.google\.com/spreadsheets/d/([a-zA-Z0-9-_]+)",
        r"https://drive\.google\.com/file/d/([a-zA-Z0-9-_]+)",
        r"https://drive\.google\.com/open\?id=([a-zA-Z0-9-_]+)"
    ]
    for pattern in patterns:
        match = re.match(pattern, url)
        if match:
            return match
    return None

def extract_sheet_id(url):
    """URLからシートIDを抽出"""
    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    
    match = re.search(r"/file/d/([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    
    match = re.search(r"[?&]id=([a-zA-Z0-9-_]+)", url)
    if match:
        return match.group(1)
    
    return None

def google_sheet_to_csv_url(sheet_url):
    """GoogleスプレッドシートURLをCSVダウンロードURLに変換"""
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return None
    
    gid_match = re.search(r"[#&]gid=([0-9]+)", sheet_url)
    gid = gid_match.group(1) if gid_match else "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def process_dataframe(df, filename):
    """データフレームの高度な処理"""
    try:
        stats = {"email_extracted": 0, "tel_extracted": 0}
        
        # ファイル名から展示会名を推測
        inferred_event_name = os.path.splitext(filename)[0]
        
        # 列名正規化（大文字小文字を無視）
        rename_dict = {}
        for col in df.columns:
            col_lower = col.lower().strip()
            for old_name, new_name in COLUMN_RENAMES.items():
                if col_lower == old_name.lower():
                    rename_dict[col] = new_name
                    break
        
        df.rename(columns=rename_dict, inplace=True)
        
        # 連絡先系列からメールアドレスとTEL抽出
        contact_cols = [c for c in df.columns if any(keyword in c for keyword in 
                       ['問い合わせ先', '連絡先', 'お問い合わせ先', 'Contact', '問合せ先'])]
        
        if contact_cols:
            # 必要カラムを事前に確保
            if 'メールアドレス' not in df.columns:
                df['メールアドレス'] = ''
            if 'Tel' not in df.columns:
                df['Tel'] = ''

            for col in contact_cols:
                # メールアドレス抽出
                temp_emails = df[col].apply(extract_email_from_text)
                email_mask = (df['メールアドレス'] == '') & (temp_emails != '')
                df.loc[email_mask, 'メールアドレス'] = temp_emails[email_mask]
                stats["email_extracted"] += email_mask.sum()
                
                # 電話番号抽出
                temp_phones = df[col].apply(extract_phone_from_text)
                phone_mask = (df['Tel'] == '') & (temp_phones != '')
                df.loc[phone_mask, 'Tel'] = temp_phones[phone_mask]
                stats["tel_extracted"] += phone_mask.sum()
        
        # 必須列がなければ追加
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        
        # 展示会名が空の場合、ファイル名から推測
        if '展示会名' in df.columns:
            df.loc[df['展示会名'] == '', '展示会名'] = inferred_event_name
        
        # 文字列列の前後空白除去
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda s: s.str.strip() if hasattr(s, 'str') else s)
        
        # 担当者名の補完
        if "担当者" in df.columns:
            df["担当者"] = df["担当者"].fillna("").replace("", "ご担当者様")
        
        # データ正規化
        if "Tel" in df.columns:
            df["Tel"] = df["Tel"].apply(normalize_phone)
        if "メールアドレス" in df.columns:
            df["メールアドレス"] = df["メールアドレス"].apply(validate_email)
        
        # 必須3列が丸ごと空ならエラー
        key_cols_check = ["メールアドレス", "Tel", "会社名"]
        if df[key_cols_check].replace("", pd.NA).isna().all().any():
            raise ValueError("必須項目（メールアドレス・Tel・会社名）の列全体が空欄です。")
        
        # ファイル名と更新日時を追加
        df['ソースファイル'] = filename
        df['更新日時'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return df, stats, None
        
    except Exception as e:
        return None, {}, str(e)

def notion_download():
    """Notion APIからファイルダウンロード"""
    st.subheader("🔗 Notion APIからダウンロード")
    
    # API設定
    col1, col2 = st.columns(2)
    with col1:
        notion_api_key = st.text_input("Notion API Key", type="password", 
                                     value=os.environ.get("NOTION_API_KEY", ""))
    with col2:
        database_id = st.text_input("Database ID", 
                                  value=os.environ.get("DATABASE_ID", ""))
    
    if st.button("Notionからダウンロード", disabled=not (notion_api_key and database_id)):
        try:
            notion = Client(auth=notion_api_key)
            
            # フィルター条件
            filter_conditions = {
                "and": [
                    {
                        "or": [
                            {"property": "メールアドレス（有無）", "select": {"equals": "あり"}},
                            {"property": "メールアドレス（有無）", "select": {"equals": "TELとURL"}},
                            {"property": "メールアドレス（有無）", "select": {"equals": "社名・住所・URL"}},
                            {"property": "メールアドレス（有無）", "select": {"equals": "Tel、住所、URL"}},
                            {"property": "メールアドレス（有無）", "select": {"equals": "社名とURL（直で企業HPリンク）"}},
                            {"property": "メールアドレス（有無）", "select": {"equals": "社名とURLのみ"}}
                        ]
                    },
                    {"property": "ファイル", "files": {"is_not_empty": True}}
                ]
            }
            
            with st.spinner("Notionからデータを取得中..."):
                # 全てのアイテムを取得
                all_items = []
                start_cursor = None
                
                while True:
                    response = notion.databases.query(
                        database_id=database_id,
                        filter=filter_conditions,
                        start_cursor=start_cursor
                    )
                    items = response.get("results", [])
                    all_items.extend(items)
                    if not response.get("has_more"):
                        break
                    start_cursor = response.get("next_cursor")
                
                st.success(f"{len(all_items)}件のアイテムが見つかりました")
                
                # ファイルダウンロード処理
                downloaded_files = []
                failed_files = []
                progress_bar = st.progress(0)
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                
                for item_idx, item in enumerate(all_items):
                    properties = item["properties"]
                    
                    # ページタイトルを取得
                    page_title = "untitled"
                    if "名前" in properties and properties["名前"]["title"]:
                        page_title = properties["名前"]["title"][0]["plain_text"] if properties["名前"]["title"] else "untitled"
                    elif "Name" in properties and properties["Name"]["title"]:
                        page_title = properties["Name"]["title"][0]["plain_text"] if properties["Name"]["title"] else "untitled"
                    
                    file_property = properties.get("ファイル")
                    if file_property and file_property["type"] == "files":
                        for file_idx, file_info in enumerate(file_property["files"]):
                            try:
                                # Notionに直接アップロードされたファイル
                                if file_info["type"] == "file":
                                    file_name = file_info["name"]
                                    file_url = file_info["file"]["url"]
                                    ext = os.path.splitext(file_name)[1].lower()
                                    
                                    if ext in [".csv", ".xlsx", ".xls"]:
                                        response = requests.get(file_url, headers=headers)
                                        response.raise_for_status()
                                        final_name = f"{os.path.splitext(file_name)[0]}_{item_idx+1}_{file_idx+1}{ext}"
                                        downloaded_files.append((final_name, response.content))
                                
                                # Googleスプレッドシート等の外部URL
                                elif file_info["type"] == "external":
                                    url = file_info["external"]["url"]
                                    if is_google_sheet_url(url):
                                        csv_url = google_sheet_to_csv_url(url)
                                        if csv_url:
                                            response = requests.get(csv_url, headers=headers)
                                            response.raise_for_status()
                                            sheet_id = extract_sheet_id(url)
                                            file_name = f"{page_title}_{sheet_id}_{item_idx+1}_{file_idx+1}.csv"
                                            downloaded_files.append((file_name, response.content))
                                        
                            except Exception as e:
                                failed_files.append(f"ファイル取得エラー: {str(e)}")
                    
                    progress_bar.progress((item_idx + 1) / len(all_items))
                
                st.session_state.notion_files = downloaded_files
                st.success(f"✅ {len(downloaded_files)}個のファイルをダウンロードしました")
                
                if failed_files:
                    st.warning(f"⚠️ {len(failed_files)}個のファイルでエラーが発生しました")
                    with st.expander("エラー詳細"):
                        for error in failed_files:
                            st.write(f"- {error}")
                
        except Exception as e:
            st.error(f"エラーが発生しました: {e}")

def file_upload_processing():
    """ファイルアップロード処理"""
    st.subheader("📁 ファイルアップロード・統合処理")
    
    # Notionからダウンロードしたファイルがある場合
    if 'notion_files' in st.session_state and st.session_state.notion_files:
        st.info(f"💾 Notionから{len(st.session_state.notion_files)}個のファイルがダウンロード済みです")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("📥 Notionファイルを処理"):
                process_files(st.session_state.notion_files, "Notion")
        with col2:
            if st.button("🗑️ Notionファイルをクリア"):
                del st.session_state.notion_files
                st.rerun()
    
    uploaded_files = st.file_uploader(
        "CSVまたはExcelファイルをアップロード",
        type=['csv', 'xlsx', 'xls'],
        accept_multiple_files=True
    )
    
    if uploaded_files:
        st.write(f"📂 アップロードされたファイル数: {len(uploaded_files)}")
        
        if st.button("🔄 アップロードファイルを処理"):
            # ファイルオブジェクトを変換
            file_data = []
            for uploaded_file in uploaded_files:
                content = uploaded_file.read()
                uploaded_file.seek(0)  # ポインタをリセット
                file_data.append((uploaded_file.name, content))
            
            process_files(file_data, "アップロード")

def process_files(file_data, source_type):
    """ファイルデータを処理"""
    processed_dfs = []
    error_files = []
    total_stats = {"email_extracted": 0, "tel_extracted": 0, "files_processed": 0}
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for idx, (filename, content) in enumerate(file_data):
        status_text.text(f"処理中: {filename}")
        
        try:
            # ファイル読み込み
            if filename.lower().endswith('.csv'):
                encoding = detect(content)["encoding"] or "utf-8"
                content_str = content.decode(encoding)
                df = pd.read_csv(io.StringIO(content_str), dtype=str, on_bad_lines="skip")
            else:
                df = pd.read_excel(io.BytesIO(content), dtype=str, engine='openpyxl')
            
            # データ処理
            processed_df, stats, error = process_dataframe(df, filename)
            
            if processed_df is not None:
                processed_dfs.append(processed_df)
                total_stats["email_extracted"] += stats["email_extracted"]
                total_stats["tel_extracted"] += stats["tel_extracted"]
                total_stats["files_processed"] += 1
                
                st.session_state.processed_files.append({
                    'filename': filename,
                    'rows': len(processed_df),
                    'email_extracted': stats["email_extracted"],
                    'tel_extracted': stats["tel_extracted"],
                    'status': '✅ 成功'
                })
            else:
                error_files.append((filename, error))
                st.session_state.processed_files.append({
                    'filename': filename,
                    'rows': 0,
                    'email_extracted': 0,
                    'tel_extracted': 0,
                    'status': f'❌ エラー: {error}'
                })
                
        except Exception as e:
            error_files.append((filename, str(e)))
            st.session_state.processed_files.append({
                'filename': filename,
                'rows': 0,
                'email_extracted': 0,
                'tel_extracted': 0,
                'status': f'❌ エラー: {e}'
            })
        
        progress_bar.progress((idx + 1) / len(file_data))
    
    # データ統合
    if processed_dfs:
        merged_df = pd.concat(processed_dfs, ignore_index=True)
        
        # 重複削除（メールアドレスベース）
        before_count = len(merged_df)
        if 'メールアドレス' in merged_df.columns:
            email_duplicates = merged_df[merged_df['メールアドレス'] != ''].duplicated(
                subset=['メールアドレス'], keep='first'
            )
            merged_df = merged_df[~email_duplicates]
        
        # 空行削除
        key_cols_check = ["メールアドレス", "Tel", "会社名"]
        empty_mask = merged_df[key_cols_check].replace("", pd.NA).isna().all(axis=1)
        merged_df = merged_df[~empty_mask]
        
        after_count = len(merged_df)
        
        # 既存データと統合
        if not st.session_state.merged_data.empty:
            st.session_state.merged_data = pd.concat([st.session_state.merged_data, merged_df], ignore_index=True)
            # 全体でも重複削除
            if 'メールアドレス' in st.session_state.merged_data.columns:
                email_duplicates = st.session_state.merged_data[st.session_state.merged_data['メールアドレス'] != ''].duplicated(
                    subset=['メールアドレス'], keep='last'
                )
                st.session_state.merged_data = st.session_state.merged_data[~email_duplicates]
        else:
            st.session_state.merged_data = merged_df
        
        # 統計情報保存
        st.session_state.processing_stats = total_stats
        
        st.success(f"""
        ✅ **{source_type}ファイル処理完了**
        - 処理ファイル数: {total_stats['files_processed']}個
        - 処理前データ数: {before_count}件
        - 重複・空行削除後: {after_count}件
        - メール抽出数: {total_stats['email_extracted']}件
        - 電話番号抽出数: {total_stats['tel_extracted']}件
        - 最終統合データ数: {len(st.session_state.merged_data)}件
        """)
    
    # エラーファイル表示
    if error_files:
        st.error("❌ 以下のファイルでエラーが発生しました:")
        for filename, error in error_files:
            st.write(f"- **{filename}**: {error}")
    
    status_text.empty()
    progress_bar.empty()

def display_processed_files():
    """処理済みファイル一覧表示"""
    if st.session_state.processed_files:
        st.subheader("📋 処理結果詳細")
        df_status = pd.DataFrame(st.session_state.processed_files)
        st.dataframe(df_status, use_container_width=True)
        
        # サマリー統計
        if st.session_state.processing_stats:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("処理成功ファイル", st.session_state.processing_stats.get('files_processed', 0))
            with col2:
                st.metric("メール抽出数", st.session_state.processing_stats.get('email_extracted', 0))
            with col3:
                st.metric("電話番号抽出数", st.session_state.processing_stats.get('tel_extracted', 0))

def data_search_and_download():
    """データ検索・ダウンロード機能"""
    if st.session_state.merged_data.empty:
        st.info("⚠️ まずファイルを処理して統合してください")
        return
        
    st.subheader("🔍 高度な検索・分析・ダウンロード")
    
    # 基本統計
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("総データ数", len(st.session_state.merged_data))
    with col2:
        unique_companies = st.session_state.merged_data['会社名'].nunique()
        st.metric("ユニーク企業数", unique_companies)
    with col3:
        unique_exhibitions = st.session_state.merged_data['展示会名'].nunique()
        st.metric("展示会数", unique_exhibitions)
    with col4:
        email_with_data = len(st.session_state.merged_data[
            (st.session_state.merged_data['メールアドレス'].notna()) & 
            (st.session_state.merged_data['メールアドレス'] != '')
        ])
        st.metric("メールアドレスあり", email_with_data)
    
    # 詳細統計
    with st.expander("📊 詳細統計情報"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**展示会別データ数（上位10位）**")
            exhibition_counts = st.session_state.merged_data['展示会名'].value_counts().head(10)
            for idx, (exhibition, count) in enumerate(exhibition_counts.items(), 1):
                st.write(f"{idx}. {exhibition}: {count}件")
        
        with col2:
            st.write("**業界別データ数（上位10位）**")
            industry_counts = st.session_state.merged_data['業界'].value_counts().head(10)
            for idx, (industry, count) in enumerate(industry_counts.items(), 1):
                st.write(f"{idx}. {industry}: {count}件")
    
    # 検索フィルター
    st.markdown("### 🎯 詳細検索フィルター")
    col1, col2 = st.columns(2)
    
    with col1:
        # 展示会名フィルター
        exhibitions = ['全て'] + sorted(st.session_state.merged_data['展示会名'].dropna().unique().tolist())
        selected_exhibitions = st.multiselect("展示会名（複数選択可）", exhibitions, default=['全て'])
        
        # 業界フィルター
        industries = ['全て'] + sorted(st.session_state.merged_data['業界'].dropna().unique().tolist())
        selected_industries = st.multiselect("業界（複数選択可）", industries, default=['全て'])
        
        # 会社名検索
        company_search = st.text_input("会社名検索（部分一致）")
    
    with col2:
        # メールアドレス有無
        email_filter = st.selectbox("メールアドレス", ['全て', 'あり', 'なし'])
        
        # 電話番号有無
        tel_filter = st.selectbox("電話番号", ['全て', 'あり', 'なし'])
        
        # 担当者検索
        contact_search = st.text_input("担当者名検索（部分一致）")
    
    # データフィルタリング
    filtered_data = st.session_state.merged_data.copy()
    
    # 複数選択対応
    if '全て' not in selected_exhibitions:
        filtered_data = filtered_data[filtered_data['展示会名'].isin(selected_exhibitions)]
    
    if '全て' not in selected_industries:
        filtered_data = filtered_data[filtered_data['業界'].isin(selected_industries)]
    
    if company_search:
        filtered_data = filtered_data[
            filtered_data['会社名'].str.contains(company_search, na=False, case=False)
        ]
    
    if contact_search:
        filtered_data = filtered_data[
            filtered_data['担当者'].str.contains(contact_search, na=False, case=False)
        ]
    
    if email_filter == 'あり':
        filtered_data = filtered_data[
            filtered_data['メールアドレス'].notna() & 
            (filtered_data['メールアドレス'] != '')
        ]
    elif email_filter == 'なし':
        filtered_data = filtered_data[
            filtered_data['メールアドレス'].isna() | 
            (filtered_data['メールアドレス'] == '')
        ]
    
    if tel_filter == 'あり':
        filtered_data = filtered_data[
            filtered_data['Tel'].notna() & 
            (filtered_data['Tel'] != '')
        ]
    elif tel_filter == 'なし':
        filtered_data = filtered_data[
            filtered_data['Tel'].isna() | 
            (filtered_data['Tel'] == '')
        ]
    
    # 検索結果表示
    st.markdown(f"### 🎯 検索結果: **{len(filtered_data)}件**")
    
    if not filtered_data.empty:
        # 結果のプレビュー設定
        preview_limit = st.slider("プレビュー表示件数", 10, 1000, 100)
        
        # データプレビュー
        st.dataframe(filtered_data.head(preview_limit), use_container_width=True)
        
        if len(filtered_data) > preview_limit:
            st.info(f"💡 プレビューは最初の{preview_limit}件を表示しています。全{len(filtered_data)}件")
        
        # ダウンロードセクション
        st.markdown("### 📥 ダウンロード")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            csv = filtered_data.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📄 CSVダウンロード",
                data=csv,
                file_name=f"exhibition_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Excelダウンロード
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                filtered_data.to_excel(writer, index=False, sheet_name='ExhibitionData')
            excel_data = output.getvalue()
            
            st.download_button(
                label="📊 Excelダウンロード",
                data=excel_data,
                file_name=f"exhibition_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        with col3:
            # メールリストのみ
            email_only = filtered_data[
                (filtered_data['メールアドレス'].notna()) & 
                (filtered_data['メールアドレス'] != '')
            ][['会社名', '担当者', 'メールアドレス', '展示会名', '業界']]
            
            email_csv = email_only.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📧 メールリスト",
                data=email_csv,
                file_name=f"email_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col4:
            # テレアポリスト
            tel_only = filtered_data[
                (filtered_data['Tel'].notna()) & 
                (filtered_data['Tel'] != '')
            ][['会社名', '担当者', 'Tel', '展示会名', '業界']]
            
            tel_csv = tel_only.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📞 テレアポリスト",
                data=tel_csv,
                file_name=f"tel_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        # 詳細統計レポート
        with st.expander("📈 検索結果の詳細分析"):
            col1, col2 = st.columns(2)
            
            with col1:
                stats_data = {
                    '項目': [
                        '総データ数', 'ユニーク企業数', '展示会数', '業界数', 
                        'メールアドレスあり', '電話番号あり', '両方あり'
                    ],
                    '件数': [
                        len(filtered_data),
                        filtered_data['会社名'].nunique(),
                        filtered_data['展示会名'].nunique(),
                        filtered_data['業界'].nunique(),
                        len(filtered_data[(filtered_data['メールアドレス'].notna()) & (filtered_data['メールアドレス'] != '')]),
                        len(filtered_data[(filtered_data['Tel'].notna()) & (filtered_data['Tel'] != '')]),
                        len(filtered_data[
                            (filtered_data['メールアドレス'].notna()) & (filtered_data['メールアドレス'] != '') &
                            (filtered_data['Tel'].notna()) & (filtered_data['Tel'] != '')
                        ])
                    ]
                }
                stats_df = pd.DataFrame(stats_data)
                st.dataframe(stats_df, use_container_width=True)
            
            with col2:
                # 更新日時別集計
                if '更新日時' in filtered_data.columns:
                    st.write("**更新日時別データ数**")
                    update_counts = filtered_data['更新日時'].value_counts().head(10)
                    for date, count in update_counts.items():
                        st.write(f"- {date}: {count}件")
    else:
        st.warning("🔍 検索条件に一致するデータがありません")

def main():
    st.title("📊 展示会リスト自動化システム（高機能版）")
    st.markdown("""
    **🚀 機能一覧:**
    - Notionからの自動ダウンロード（Googleスプレッドシート対応）
    - 高度なデータ抽出（連絡先からメール・電話番号自動抽出）
    - 列名の自動正規化・データクリーニング
    - 重複削除・データ統合
    - 高度な検索・フィルタリング
    - メールリスト・テレアポリスト生成
    """)
    
    # サイドバーでモード選択
    st.sidebar.title("🔧 機能選択")
    mode = st.sidebar.radio(
        "処理を選択してください:",
        ["🔗 Notion連携", "📁 ファイル処理", "🔍 データ検索・分析"]
    )
    
    if mode == "🔗 Notion連携":
        notion_download()
    elif mode == "📁 ファイル処理":
        file_upload_processing()
        display_processed_files()
    elif mode == "🔍 データ検索・分析":
        data_search_and_download()
    
    # サイドバーの統計情報
    if not st.session_state.merged_data.empty:
        st.sidebar.markdown("---")
        st.sidebar.markdown("📊 **現在のデータ状況**")
        st.sidebar.metric("総データ数", len(st.session_state.merged_data))
        st.sidebar.metric("企業数", st.session_state.merged_data['会社名'].nunique())
        email_count = len(st.session_state.merged_data[
            (st.session_state.merged_data['メールアドレス'].notna()) & 
            (st.session_state.merged_data['メールアドレス'] != '')
        ])
        st.sidebar.metric("メールあり", email_count)
    
    # データリセット
    if st.sidebar.button("🔄 全データをリセット"):
        st.session_state.merged_data = pd.DataFrame()
        st.session_state.processed_files = []
        st.session_state.processing_stats = {}
        if 'notion_files' in st.session_state:
            del st.session_state.notion_files
        st.sidebar.success("✅ データをリセットしました")
        st.rerun()

if __name__ == "__main__":
    main()
