#!/usr/bin/env python3
"""
update.py - 展示会リスト月次自動更新スクリプト
毎月月末に実行され、当月の新規ファイルのみを処理して統合
重複削除では古いデータを優先的に削除
"""

import os
import pandas as pd
import requests
import re
from datetime import datetime, timedelta
from notion_client import Client
from charset_normalizer import detect
import tempfile
import json
import logging
import hashlib
from calendar import monthrange

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 設定
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
DATABASE_ID = os.environ.get("DATABASE_ID")
FORCE_FULL_UPDATE = os.environ.get("FORCE_FULL_UPDATE", "false").lower() == "true"

OUTPUT_DIR = "data"
MERGED_FILE = os.path.join(OUTPUT_DIR, "merged_exhibition_data.xlsx")
MONTHLY_FILE = os.path.join(OUTPUT_DIR, "monthly_new_data.xlsx")
PROCESSED_FILES_LOG = os.path.join(OUTPUT_DIR, "processed_files.json")
UPDATE_LOG_FILE = os.path.join(OUTPUT_DIR, "monthly_update_log.json")

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
}

REQUIRED_COLUMNS = ["メールアドレス", "展示会名", "担当者", "業界", "Tel", "会社名"]
KEY_COLS = ["展示会名", "業界", "会社名"]

def create_output_dir():
    """出力ディレクトリを作成"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

def get_current_month_range():
    """今月の開始日と終了日を取得"""
    now = datetime.now()
    first_day = datetime(now.year, now.month, 1)
    last_day_of_month = monthrange(now.year, now.month)[1]
    last_day = datetime(now.year, now.month, last_day_of_month, 23, 59, 59)
    return first_day, last_day

def load_processed_files():
    """処理済みファイルのログを読み込み"""
    if os.path.exists(PROCESSED_FILES_LOG):
        try:
            with open(PROCESSED_FILES_LOG, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_processed_files(processed_files):
    """処理済みファイルのログを保存"""
    with open(PROCESSED_FILES_LOG, 'w', encoding='utf-8') as f:
        json.dump(processed_files, f, ensure_ascii=False, indent=2)

def generate_file_hash(content):
    """ファイルコンテンツのハッシュを生成"""
    return hashlib.md5(content).hexdigest()

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

def fetch_new_files_from_notion(notion, database_id, processed_files):
    """Notionから今月の新規ファイルのみを取得"""
    logging.info("Notionから今月の新規ファイルを検索中...")
    
    # 今月の範囲を取得
    first_day, last_day = get_current_month_range()
    
    # フィルター条件（抽出日が今月のもの）
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
            {"property": "ファイル", "files": {"is_not_empty": True}},
            # 抽出日が今月のもの
            {"property": "抽出日", "date": {
                "on_or_after": first_day.strftime('%Y-%m-%d'), 
                "on_or_before": last_day.strftime('%Y-%m-%d')
            }}
        ]
    }

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

    # 新規ファイルのみフィルタリング
    new_files = []
    for item in all_items:
        properties = item["properties"]
        file_property = properties.get("ファイル")
        
        if file_property and file_property["type"] == "files":
            for file_info in file_property["files"]:
                if file_info["type"] == "file":
                    file_url = file_info["file"]["url"]
                    file_name = file_info["name"]
                elif file_info["type"] == "external":
                    file_url = file_info["external"]["url"]
                    file_name = extract_sheet_id(file_url) or "unknown"
                else:
                    continue
                
                # ファイルが既に処理済みかチェック
                file_key = f"{item['id']}_{file_url}"
                if file_key not in processed_files:
                    new_files.append(item)
                    break

    logging.info(f"今月の抽出日条件: {first_day.strftime('%Y-%m-%d')} ～ {last_day.strftime('%Y-%m-%d')}")
    logging.info(f"今月の新規アイテム: {len(new_files)}件")
    return new_files

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
        df['処理月'] = datetime.now().strftime('%Y-%m')
        
        return df, stats, None
        
    except Exception as e:
        return None, {}, str(e)

def download_and_process_new_files(items, processed_files):
    """新規ファイルをダウンロードして処理"""
    logging.info("新規ファイルをダウンロード・処理中...")
    
    processed_dfs = []
    error_count = 0
    success_count = 0
    total_stats = {"email_extracted": 0, "tel_extracted": 0}
    new_processed_files = processed_files.copy()
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    for item_idx, item in enumerate(items):
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
                    file_content = None
                    file_name = ""
                    file_url = ""
                    
                    # Notionに直接アップロードされたファイル
                    if file_info["type"] == "file":
                        file_name = file_info["name"]
                        file_url = file_info["file"]["url"]
                        ext = os.path.splitext(file_name)[1].lower()
                        
                        if ext in [".csv", ".xlsx", ".xls"]:
                            response = requests.get(file_url, headers=headers)
                            response.raise_for_status()
                            file_content = response.content
                            final_name = f"{os.path.splitext(file_name)[0]}_{item_idx+1}_{file_idx+1}{ext}"
                    
                    # Googleスプレッドシート等の外部URL
                    elif file_info["type"] == "external":
                        file_url = file_info["external"]["url"]
                        if is_google_sheet_url(file_url):
                            csv_url = google_sheet_to_csv_url(file_url)
                            if csv_url:
                                response = requests.get(csv_url, headers=headers)
                                response.raise_for_status()
                                file_content = response.content
                                sheet_id = extract_sheet_id(file_url)
                                final_name = f"{page_title}_{sheet_id}_{item_idx+1}_{file_idx+1}.csv"
                    
                    if file_content and final_name:
                        # ファイルハッシュを生成してチェック
                        file_hash = generate_file_hash(file_content)
                        file_key = f"{item['id']}_{file_url}"
                        
                        if file_key not in processed_files or processed_files[file_key].get('hash') != file_hash:
                            # ファイルを処理
                            df = process_file_content(file_content, final_name)
                            if df is not None:
                                processed_df, stats, error = process_dataframe(df, final_name)
                                if processed_df is not None:
                                    processed_dfs.append(processed_df)
                                    total_stats["email_extracted"] += stats["email_extracted"]
                                    total_stats["tel_extracted"] += stats["tel_extracted"]
                                    success_count += 1
                                    
                                    # 処理済みファイルとして記録
                                    new_processed_files[file_key] = {
                                        'filename': final_name,
                                        'hash': file_hash,
                                        'processed_date': datetime.now().isoformat(),
                                        'rows': len(processed_df)
                                    }
                                    
                                    logging.info(f"処理成功: {final_name} ({len(processed_df)}行)")
                                else:
                                    error_count += 1
                                    logging.error(f"データ処理エラー: {final_name} - {error}")
                            else:
                                error_count += 1
                                logging.error(f"ファイル処理エラー: {final_name}")
                        else:
                            logging.info(f"スキップ（既処理済み）: {final_name}")
                    
                except Exception as e:
                    error_count += 1
                    logging.error(f"ダウンロードエラー: {str(e)}")
    
    logging.info(f"処理完了: 成功 {success_count}件, エラー {error_count}件")
    logging.info(f"メール抽出: {total_stats['email_extracted']}件, 電話番号抽出: {total_stats['tel_extracted']}件")
    
    # 処理済みファイルログを更新
    save_processed_files(new_processed_files)
    
    return processed_dfs, total_stats

def process_file_content(file_content, filename):
    """ファイル内容を処理してDataFrameを返す"""
    try:
        # 一時ファイルに保存
        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(filename)[1]) as tmp_file:
            tmp_file.write(file_content)
            tmp_file.flush()
            
            # ファイル読み込み
            if filename.lower().endswith('.csv'):
                # 文字コード判定
                encoding = detect(file_content)["encoding"] or "utf-8"
                df = pd.read_csv(tmp_file.name, dtype=str, encoding=encoding, on_bad_lines="skip")
            else:
                df = pd.read_excel(tmp_file.name, dtype=str, engine='openpyxl')
            
            # 一時ファイル削除
            os.unlink(tmp_file.name)
        
        return df
        
    except Exception as e:
        if 'tmp_file' in locals():
            try:
                os.unlink(tmp_file.name)
            except:
                pass
        logging.error(f"ファイル処理エラー ({filename}): {e}")
        return None

def load_existing_data():
    """既存の統合データを読み込み"""
    if os.path.exists(MERGED_FILE):
        try:
            df = pd.read_excel(MERGED_FILE, dtype=str, engine='openpyxl')
            logging.info(f"既存データ読み込み完了: {len(df)}行")
            return df
        except Exception as e:
            logging.warning(f"既存データの読み込みに失敗: {e}")
    
    return pd.DataFrame()

def merge_with_existing_data(new_dfs, existing_df):
    """新規データと既存データを統合し、重複削除"""
    if not new_dfs:
        logging.info("新規データがありません")
        return existing_df
    
    # 新規データを統合
    new_data = pd.concat(new_dfs, ignore_index=True)
    logging.info(f"新規データ: {len(new_data)}行")
    
    if existing_df.empty:
        merged_df = new_data
    else:
        # 既存データと結合
        merged_df = pd.concat([existing_df, new_data], ignore_index=True)
    
    before_count = len(merged_df)
    
    # 重複削除（古いデータを削除、新しいデータを保持）
    # メールアドレスベースの重複削除
    if 'メールアドレス' in merged_df.columns:
        email_mask = (merged_df['メールアドレス'].notna()) & (merged_df['メールアドレス'] != '')
        email_duplicates = merged_df[email_mask].duplicated(subset=['メールアドレス'], keep='last')
        merged_df = merged_df[~email_duplicates]
        email_removed = email_duplicates.sum()
        logging.info(f"メールアドレス重複削除: {email_removed}件")
    
    # 会社名+展示会名ベースの重複削除
    key_duplicates = merged_df.duplicated(subset=['会社名', '展示会名'], keep='last')
    merged_df = merged_df[~key_duplicates]
    key_removed = key_duplicates.sum()
    logging.info(f"会社名+展示会名重複削除: {key_removed}件")
    
    # 空行削除
    key_cols_check = ["メールアドレス", "Tel", "会社名"]
    empty_mask = merged_df[key_cols_check].replace("", pd.NA).isna().all(axis=1)
    merged_df = merged_df[~empty_mask]
    empty_removed = empty_mask.sum()
    logging.info(f"空行削除: {empty_removed}件")
    
    after_count = len(merged_df)
    logging.info(f"統合結果: {before_count} → {after_count}行 ({before_count - after_count}件削除)")
    
    return merged_df

def save_monthly_update_log(stats, new_files_count, final_count):
    """月次更新ログを保存"""
    log_data = {
        "update_date": datetime.now().isoformat(),
        "month": datetime.now().strftime('%Y-%m'),
        "new_files_processed": new_files_count,
        "email_extracted": stats.get("email_extracted", 0),
        "tel_extracted": stats.get("tel_extracted", 0),
        "final_total_rows": final_count,
        "merged_file": MERGED_FILE
    }
    
    with open(UPDATE_LOG_FILE, 'w', encoding='utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
    
    logging.info(f"更新ログ保存完了: {UPDATE_LOG_FILE}")

def main():
    """メイン処理"""
    current_time = datetime.now()
    logging.info(f"月次展示会リスト更新開始: {current_time}")
    logging.info(f"強制全更新モード: {FORCE_FULL_UPDATE}")
    
    # 環境変数チェック
    if not NOTION_API_KEY or not DATABASE_ID:
        logging.error("エラー: NOTION_API_KEY または DATABASE_ID が設定されていません")
        return
    
    try:
        # 出力ディレクトリ作成
        create_output_dir()
        
        # Notion クライアント初期化
        notion = Client(auth=NOTION_API_KEY)
        
        # 処理済みファイルログを読み込み
        processed_files = load_processed_files()
        
        if FORCE_FULL_UPDATE:
            logging.info("強制全更新モード: 全データを再処理します")
            processed_files = {}  # 処理済みファイルログをクリア
        
        # 今月の新規ファイルを取得
        new_items = fetch_new_files_from_notion(notion, DATABASE_ID, processed_files)
        
        if not new_items and not FORCE_FULL_UPDATE:
            logging.info("今月の新規ファイルが見つかりませんでした。処理を終了します。")
            return
        
        # 新規ファイルをダウンロード・処理
        new_dfs, stats = download_and_process_new_files(new_items, processed_files)
        
        # 今月の新規データを保存
        if new_dfs:
            monthly_data = pd.concat(new_dfs, ignore_index=True)
            monthly_data.to_excel(MONTHLY_FILE, index=False)
            logging.info(f"今月の新規データ保存: {MONTHLY_FILE} ({len(monthly_data)}行)")
        
        # 既存データを読み込み
        existing_data = load_existing_data()
        
        # データ統合と重複削除
        final_data = merge_with_existing_data(new_dfs, existing_data)
        
        # 最終データを保存
        if not final_data.empty:
            final_data.to_excel(MERGED_FILE, index=False)
            logging.info(f"統合データ保存完了: {MERGED_FILE} ({len(final_data)}行)")
            
            # 更新ログ保存
            save_monthly_update_log(stats, len(new_items), len(final_data))
            
            logging.info(f"月次更新完了: 新規ファイル {len(new_items)}件, 最終データ数 {len(final_data)}件")
        else:
            logging.warning("統合データが空です")
            
    except Exception as e:
        logging.error(f"エラーが発生しました: {e}")
        raise

if __name__ == "__main__":
    main()
