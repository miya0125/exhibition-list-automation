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
import csv
from charset_normalizer import detect
from notion_client import Client
import json

# ページ設定
st.set_page_config(
    page_title="展示会リスト自動化システム（高機能版）",
    page_icon="📊",
    layout="wide"
)

# APIキーのデフォルト値設定
DEFAULT_NOTION_API_KEY = "APIを入力"
DEFAULT_DATABASE_ID = "ID入力"
DEFAULT_GOOGLE_SHEETS_API_KEY = "APIを入力"

# セッション状態の初期化
if 'merged_data' not in st.session_state:
    st.session_state.merged_data = pd.DataFrame()
if 'processed_files' not in st.session_state:
    st.session_state.processed_files = []
if 'processing_stats' not in st.session_state:
    st.session_state.processing_stats = {}

# メモリ使用量警告設定
if 'memory_warning_shown' not in st.session_state:
    st.session_state.memory_warning_shown = False

# 拡張版列名マッピング（日本語のバリエーションを大幅追加）
COLUMN_RENAMES = {
    # 会社名関連
    '会社名': '会社名', '企業名': '会社名', '出展社名': '会社名',
    '出展社': '会社名', '法人名': '会社名', 'COMPANY': '会社名', 
    '社名': '会社名', 'Company': '会社名', 'company': '会社名',
    '出展企業名': '会社名', '出展者名': '会社名', 'Corporate Name': '会社名',
    '組織名': '会社名', 'Organization': '会社名', '団体名': '会社名',
    '会社名タイトル': '会社名',
    
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
    
    # 展示会初日関連
    '展示会初日': '展示会初日', '開始日': '展示会初日', '初日': '展示会初日',
    '開催日': '展示会初日', 'Start Date': '展示会初日', '会期初日': '展示会初日',
    
    # 電話番号関連（より幅広いパターンを追加）
    '電話番号': 'Tel', 'Tel': 'Tel', '電話': 'Tel', 'TEL': 'Tel', 'Phone': 'Tel', 
    'TEL_FAX': 'Tel', 'TELとFAX': 'Tel', 'tel': 'Tel', 'phone': 'Tel',
    'Telephone': 'Tel', 'Phone Number': 'Tel', '連絡先電話': 'Tel',
    'TEL番号': 'Tel', 'Tel番号': 'Tel', '℡': 'Tel', 'ＴＥＬ': 'Tel',
    
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

def fix_email_address(email):
    """メールアドレスを修正する関数"""
    if pd.isna(email) or str(email).strip() == '':
        return ''
    
    email = str(email).strip()
    
    # 全角＠を半角@に変換
    email = email.replace('＠', '@')
    
    # カンマをピリオドに変換（ドメイン部分のみ）
    if '@' in email:
        local_part, domain_part = email.split('@', 1)
        domain_part = domain_part.replace(',', '.')
        email = f"{local_part}@{domain_part}"
    
    # よくある誤字の修正
    replacements = [
        ('@@', '@'),
        ('..', '.'),
        ('.comm', '.com'),
        ('co.jo', 'co.jp'),
        ('co..jp', 'co.jp'),
        ('co.jｐ', 'co.jp'),
        ('cojp', 'co.jp'),
        ('.con', '.com'),
        ('.cm', '.com'),
        ('.cpm', '.com'),
        ('gmail.co', 'gmail.com'),
        ('yahoo.co,jp', 'yahoo.co.jp'),
    ]
    
    for wrong, correct in replacements:
        email = email.replace(wrong, correct)
    
    # 不要な文字を削除
    email = email.replace('?', '')
    email = email.replace(' ', '')
    email = email.replace('　', '')
    email = email.replace('\n', '')
    email = email.replace('\r', '')
    email = email.replace('\t', '')
    
    # メールアドレスに含まれるべきでない文字を削除
    email = re.sub(r'[^a-zA-Z0-9@._\-+]', '', email)
    
    # @が複数ある場合は最初の@以外を削除
    at_count = email.count('@')
    if at_count > 1:
        parts = email.split('@')
        email = parts[0] + '@' + ''.join(parts[1:])
    
    # @の前後に何もない場合は無効
    if '@' not in email or email.startswith('@') or email.endswith('@'):
        return ''
    
    # ドメイン部分の修正
    if '@' in email:
        local, domain = email.split('@', 1)
        
        # ドメインが数字だけの場合は無効
        if domain.isdigit():
            return ''
        
        # ドメインにピリオドがない場合、一般的なドメインを推測
        if '.' not in domain:
            if domain == 'gmail':
                domain = 'gmail.com'
            elif domain == 'yahoo':
                domain = 'yahoo.co.jp'
            elif domain in ['hotmail', 'outlook']:
                domain = domain + '.com'
        
        # ドメインの最後がピリオドで終わる場合は削除
        domain = domain.rstrip('.')
        
        # ドメインの先頭がピリオドの場合は削除
        domain = domain.lstrip('.')
        
        email = f"{local}@{domain}"
    
    # 最終的な妥当性チェック
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return ''
    
    return email.lower()

def merge_sns_columns(df):
    """SNS関連の重複列を統合する関数"""
    try:
        # SNSごとの列パターンを定義
        sns_patterns = {
            'YouTube': ['YouTube', 'Youtube', 'YouTube企業URL', 'Youtube企業URL', 'youtube'],
            'Instagram': ['Instagram', 'instagram', 'Instagram企業URL', 'インスタグラム'],
            'Facebook': ['Facebook', 'facebook', 'Facebook企業URL', 'フェイスブック'],
            'Twitter': ['Twitter', 'twitter', 'Twitter企業URL', 'X', 'X_twitter', 'x'],
            'LinkedIn': ['LinkedIn', 'linkedin', 'linkedin企業URL', 'リンクトイン'],
            'TikTok': ['TikTok', 'tiktok', 'TikTok企業URL', 'ティックトック']
        }
        
        # 各SNSについて列を統合
        for sns_name, patterns in sns_patterns.items():
            # 該当する列を探す
            matching_cols = []
            for col in df.columns:
                if any(pattern.lower() in str(col).lower() for pattern in patterns):
                    matching_cols.append(col)
            
            # 複数の列がある場合は統合
            if len(matching_cols) > 1:
                # 最初の非空値を取得
                df[sns_name] = df[matching_cols].apply(
                    lambda row: next((val for val in row if pd.notna(val) and str(val).strip()), ''), 
                    axis=1
                )
                # 元の列を削除
                df = df.drop(columns=matching_cols)
                st.info(f"🔄 {sns_name}列を統合しました（{len(matching_cols)}列 → 1列）")
            elif len(matching_cols) == 1:
                # 1列のみの場合は列名を統一
                df = df.rename(columns={matching_cols[0]: sns_name})
        
        # 重複する住所、電話番号、メールアドレス列も統合
        duplicate_patterns = {
            'Address': ['Address', 'Address_dup1', '住所', '住所.1', '住所すべて'],
            'Tel': ['Tel', 'Tel_dup1', 'Tel_dup2', 'Tel_dup3', '電話番号', '電話'],
            'メールアドレス': ['メールアドレス', 'メールアドレス_dup1', 'メールアドレス２'],
            'Website': ['Website', 'Website_dup1', 'WEB_Site', 'ホームページURL', 'URL', 'URL2', 'URL_コピー'],
            '会社名': ['会社名', '会社名_dup1', '会社名_dup2', '会社名_dup3', '会社名_dup4'],
            '業界': ['業界', '業界_dup1']
        }
        
        for target_name, patterns in duplicate_patterns.items():
            matching_cols = [col for col in df.columns if col in patterns]
            
            if len(matching_cols) > 1:
                # 最初の非空値を取得
                df[target_name] = df[matching_cols].apply(
                    lambda row: next((val for val in row if pd.notna(val) and str(val).strip()), ''), 
                    axis=1
                )
                # 元の列を削除（ターゲット名は保持）
                cols_to_drop = [col for col in matching_cols if col != target_name]
                df = df.drop(columns=cols_to_drop)
                st.info(f"🔄 {target_name}列を統合しました（{len(matching_cols)}列 → 1列）")
        
        # メールアドレス列の修正を実行
        email_columns = []
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['mail', 'email', 'メール', 'e-mail']):
                email_columns.append(col)
        
        if email_columns:
            fixed_count = 0
            for col in email_columns:
                original_emails = df[col].copy()
                df[col] = df[col].apply(fix_email_address)
                
                # 修正された件数をカウント
                for orig, fixed in zip(original_emails, df[col]):
                    orig_str = str(orig).strip() if pd.notna(orig) else ''
                    fixed_str = str(fixed).strip() if pd.notna(fixed) else ''
                    if orig_str != fixed_str and orig_str != '':
                        fixed_count += 1
            
            if fixed_count > 0:
                st.info(f"📧 メールアドレスを修正しました（{fixed_count}件）")
        
        return df
        
    except Exception as e:
        st.warning(f"SNS列統合中にエラーが発生しました: {e}")
        return df

def google_sheet_to_csv_with_api(sheet_url, google_api_key=None):
    """Google Sheets APIを使用してCSVデータを取得"""
    if not google_api_key:
        return None
    
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return None
    
    # Google Sheets API URL
    api_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/A:ZZ?key={google_api_key}"
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        
        data = response.json()
        values = data.get('values', [])
        
        if not values:
            return None
        
        # CSVフォーマットに変換
        import csv
        output = io.StringIO()
        writer = csv.writer(output)
        for row in values:
            writer.writerow(row)
        
        return output.getvalue().encode('utf-8')
        
    except Exception as e:
        st.warning(f"Google Sheets API エラー: {e}")
        return None

def google_sheet_to_csv_url(sheet_url):
    """公開Googleスプレッドシート用のCSVダウンロードURL"""
    sheet_id = extract_sheet_id(sheet_url)
    if not sheet_id:
        return None
    
    gid_match = re.search(r"[#&]gid=([0-9]+)", sheet_url)
    gid = gid_match.group(1) if gid_match else "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def safe_concat_dataframes(chunk, debug_mode=False):
    """安全にDataFrameを結合する関数"""
    if not chunk:
        return pd.DataFrame()
    
    if len(chunk) == 1:
        return chunk[0].copy().reset_index(drop=True)
    
    # 各DataFrameを事前処理
    safe_chunk = []
    for i, df in enumerate(chunk):
        try:
            # DataFrameのコピーを作成
            df_safe = df.copy()
            
            # インデックスを完全にリセット
            df_safe = df_safe.reset_index(drop=True)
            
            # 列名を文字列に統一
            df_safe.columns = [str(col) for col in df_safe.columns]
            
            # 列名の重複をチェック・修正
            seen_cols = {}
            new_cols = []
            for col in df_safe.columns:
                if col in seen_cols:
                    seen_cols[col] += 1
                    new_col = f"{col}_dup{seen_cols[col]}"
                    new_cols.append(new_col)
                    if debug_mode:
                        st.warning(f"重複列名修正: {col} → {new_col}")
                else:
                    seen_cols[col] = 0
                    new_cols.append(col)
            
            df_safe.columns = new_cols
            safe_chunk.append(df_safe)
            
        except Exception as e:
            if debug_mode:
                st.error(f"DataFrame{i}の前処理エラー: {e}")
            continue
    
    if not safe_chunk:
        return pd.DataFrame()
    
    # 結合を試行
    try:
        # 方法1: 通常のconcat
        merged = pd.concat(safe_chunk, ignore_index=True, sort=False)
        return merged.reset_index(drop=True)
    except Exception as e1:
        if debug_mode:
            st.warning(f"通常のconcat失敗: {e1}")
        
        # 方法2: 逐次的な結合
        try:
            merged = safe_chunk[0].copy()
            for df in safe_chunk[1:]:
                # 列を揃える
                for col in merged.columns:
                    if col not in df.columns:
                        df[col] = ""
                for col in df.columns:
                    if col not in merged.columns:
                        merged[col] = ""
                
                # 同じ列順にする
                df = df[merged.columns]
                
                # 結合
                merged = pd.concat([merged, df], ignore_index=True, sort=False)
                merged = merged.reset_index(drop=True)
            
            return merged
            
        except Exception as e2:
            if debug_mode:
                st.error(f"逐次結合も失敗: {e2}")
            
            # 方法3: 最後の手段として最初のDataFrameのみ返す
            return safe_chunk[0].copy().reset_index(drop=True) if safe_chunk else pd.DataFrame()

def process_dataframe(df, filename):
    """データフレームの高度な処理（安全版）"""
    try:
        stats = {"email_extracted": 0, "tel_extracted": 0}
        
        # ファイル名から展示会名を推測
        inferred_event_name = os.path.splitext(filename)[0]
        
        # 重要：最初にインデックスリセット
        df = df.reset_index(drop=True)
        
        # 不要な列を削除
        columns_to_drop = []
        for col in df.columns:
            col_lower = str(col).lower().strip()
            # ブース番号、小間番号、ロゴURLを削除
            if any(keyword in col_lower for keyword in ['ブース番号', '小間番号', 'ロゴurl', 'ロゴ url', 'logo', 'booth', '小間', 'ブース']):
                columns_to_drop.append(col)
                st.info(f"🗑️ 不要列削除: {col}")
        
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
        
        # デバッグ: 元の列名を表示
        st.write(f"🔍 **{filename}の元の列名:**")
        st.write(list(df.columns))
        
        # Step 1: 重複した列名をチェック・修正
        st.write("🔧 **重複列名チェック中...**")
        original_columns = list(df.columns)
        
        # 重複した列名を検出
        duplicate_counts = {}
        new_columns = []
        
        for col in original_columns:
            col_str = str(col).strip()
            if col_str in duplicate_counts:
                duplicate_counts[col_str] += 1
                new_col_name = f"{col_str}_dup{duplicate_counts[col_str]}"
                new_columns.append(new_col_name)
                st.warning(f"⚠️ 重複列名検出: '{col_str}' → '{new_col_name}'")
            else:
                duplicate_counts[col_str] = 0
                new_columns.append(col_str)
        
        # 列名を更新
        df.columns = new_columns
        
        # 列名を文字列に統一
        df.columns = [str(col) for col in df.columns]
        
        # インデックスリセット
        df = df.reset_index(drop=True)
        
        # Step 2: 空の列や無効な列を削除
        st.write("🧹 **空列の削除中...**")
        original_shape = df.shape
        
        # 完全に空の列を削除
        df = df.dropna(axis=1, how='all')
        
        # 列名が空や無効な列を削除
        valid_columns = []
        for i, col in enumerate(df.columns):
            col_str = str(col).strip()
            if col_str and col_str.lower() not in ['unnamed', 'nan', 'null', '']:
                valid_columns.append(col)
            else:
                st.info(f"🗑️ 無効な列を削除: インデックス{i} '{col}'")
        
        df = df[valid_columns]
        
        if df.shape != original_shape:
            st.info(f"📊 形状変更: {original_shape} → {df.shape}")
        
        # サンプルデータも表示（デバッグ用）
        if len(df) > 0:
            st.write(f"📄 **最初の3行のサンプルデータ:**")
            st.dataframe(df.head(3), use_container_width=True)
        
        # Step 3: インデックスをリセット（重複対策）
        df = df.reset_index(drop=True)
        
        # 列名正規化（厳密なマッピングのみ使用）
        rename_dict = {}
        for col in df.columns:
            col_clean = str(col).strip()
            col_lower = col_clean.lower()
            
            # 1. 完全一致による変換（最優先）
            for old_name, new_name in COLUMN_RENAMES.items():
                if col_lower == old_name.lower():
                    rename_dict[col] = new_name
                    break
            
            # 2. 厳密な部分マッチング（必要最小限のみ）
            if col not in rename_dict:
                # 電話番号系（TEL、電話を含む）
                if any(keyword in col_lower for keyword in ['tel', 'ＴＥＬ']) and 'url' not in col_lower:
                    if '電話' in col_clean or 'tel' in col_lower.replace('ＴＥＬ', 'tel'):
                        rename_dict[col] = 'Tel'
                
                # メールアドレス系
                elif any(keyword in col_lower for keyword in ['mail', 'メール']) and 'url' not in col_lower:
                    if 'address' in col_lower or 'アドレス' in col_clean:
                        rename_dict[col] = 'メールアドレス'
                
                # 会社名系（URLを除外）
                elif 'url' not in col_lower and 'link' not in col_lower:
                    if any(keyword in col_clean for keyword in ['会社', '企業', '社名']):
                        rename_dict[col] = '会社名'
                    elif any(keyword in col_lower for keyword in ['company']) and len(col_clean) < 20:
                        rename_dict[col] = '会社名'
                
                # ウェブサイト系（企業の公式サイトのみ）
                elif any(keyword in col_clean for keyword in ['ウェブサイト', 'ホームページ', 'HP']):
                    rename_dict[col] = 'Website'
                elif col_lower in ['url', 'website', 'homepage'] and len(col_clean) < 15:
                    rename_dict[col] = 'Website'
                
                # 担当者系
                elif 'url' not in col_lower and any(keyword in col_clean for keyword in ['担当', '氏名', '名前']):
                    if '会社' not in col_clean and '企業' not in col_clean:
                        rename_dict[col] = '担当者'
                
                # 住所系
                elif 'url' not in col_lower and any(keyword in col_clean for keyword in ['住所', '所在地']):
                    rename_dict[col] = 'Address'
                
                # 業界系
                elif 'url' not in col_lower and any(keyword in col_clean for keyword in ['業界', '業種', '分野']):
                    rename_dict[col] = '業界'
                
                # 展示会初日系
                elif 'url' not in col_lower and any(keyword in col_clean for keyword in ['展示会初日', '開始日', '初日', '開催日']):
                    rename_dict[col] = '展示会初日'
        
        # SNS URLs や その他のURLを明示的に除外
        excluded_patterns = [
            'facebook', 'twitter', 'instagram', 'linkedin', 'tiktok', 'youtube',
            'sns', 'social', '抽出元', 'ロゴ', 'logo'
        ]
        
        # 除外パターンに該当する列は変換しない
        for col, new_name in list(rename_dict.items()):
            col_lower = col.lower()
            if any(pattern in col_lower for pattern in excluded_patterns):
                if 'url' in col_lower or 'link' in col_lower:
                    del rename_dict[col]  # SNS URLは変換しない
        
        # デバッグ: 列名変換を表示
        if rename_dict:
            st.write(f"🔄 **列名変換:**")
            for old, new in rename_dict.items():
                st.write(f"- '{old}' → '{new}'")
        else:
            st.write("ℹ️ **列名変換なし** (既に適切な列名)")
        
        # 除外された列も表示
        excluded_cols = [col for col in df.columns if col not in rename_dict and 
                        any(pattern in col.lower() for pattern in excluded_patterns + ['url', 'link'])]
        if excluded_cols:
            st.write(f"🚫 **除外された列（変換しない）:**")
            for col in excluded_cols:
                st.write(f"- '{col}' (SNS/その他のURL)")
        
        # Step 4: 列名変換実行（再度重複チェック）
        st.write("🔄 **列名変換実行中...**")
        
        # 変換後の列名が重複しないかチェック
        new_column_names = []
        used_names = set()
        
        for col in df.columns:
            if col in rename_dict:
                new_name = rename_dict[col]
                if new_name in used_names:
                    # 重複する場合は番号を追加
                    counter = 2
                    unique_name = f"{new_name}_{counter}"
                    while unique_name in used_names:
                        counter += 1
                        unique_name = f"{new_name}_{counter}"
                    new_column_names.append(unique_name)
                    used_names.add(unique_name)
                    st.warning(f"⚠️ 変換後重複回避: '{col}' → '{unique_name}'")
                else:
                    new_column_names.append(new_name)
                    used_names.add(new_name)
            else:
                # 変換しない列もユニーク性をチェック
                original_name = col
                if original_name in used_names:
                    counter = 2
                    unique_name = f"{original_name}_{counter}"
                    while unique_name in used_names:
                        counter += 1
                        unique_name = f"{original_name}_{counter}"
                    new_column_names.append(unique_name)
                    used_names.add(unique_name)
                    st.warning(f"⚠️ 元列名重複回避: '{original_name}' → '{unique_name}'")
                else:
                    new_column_names.append(original_name)
                    used_names.add(original_name)
        
        # 列名を一括更新
        df.columns = new_column_names
        
        # 列名を文字列に統一
        df.columns = [str(col) for col in df.columns]
        
        # インデックスリセット
        df = df.reset_index(drop=True)
        
        st.write(f"✅ **変換後の列名:**")
        st.write(list(df.columns))

        # 連絡先系列からメールアドレスとTEL抽出
        contact_cols = [c for c in df.columns if any(keyword in str(c).lower() for keyword in 
                       ['問い合わせ先', '連絡先', 'お問い合わせ先', 'contact', '問合せ先', '連絡'])]
        
        # デバッグ: 連絡先関連の列を表示
        if contact_cols:
            st.write(f"📞 **連絡先関連の列:** {contact_cols}")
        
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
                if email_mask.any():
                    df.loc[email_mask, 'メールアドレス'] = temp_emails[email_mask]
                    stats["email_extracted"] += email_mask.sum()
                    st.info(f"📧 {col}から{email_mask.sum()}件のメールアドレスを抽出")
                
                # 電話番号抽出
                temp_phones = df[col].apply(extract_phone_from_text)
                phone_mask = (df['Tel'] == '') & (temp_phones != '')
                if phone_mask.any():
                    df.loc[phone_mask, 'Tel'] = temp_phones[phone_mask]
                    stats["tel_extracted"] += phone_mask.sum()
                    st.info(f"📞 {col}から{phone_mask.sum()}件の電話番号を抽出")

        # 必須列がなければ追加
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        
        # 展示会初日列も追加
        if '展示会初日' not in df.columns:
            df['展示会初日'] = ""
        
        # デバッグ: 各必須列のデータ状況を確認
        st.write(f"📊 **必須列のデータ状況:**")
        key_cols_check = ["メールアドレス", "Tel", "会社名"]
        for col in key_cols_check:
            if col in df.columns:
                non_empty_count = len(df[df[col].notna() & (df[col] != '')])
                total_count = len(df)
                st.write(f"- {col}: {non_empty_count}/{total_count}件（非空）")
                
                # サンプルデータを表示
                sample_data = df[df[col].notna() & (df[col] != '')][col].head(3).tolist()
                if sample_data:
                    st.write(f"  サンプル: {sample_data}")
            else:
                st.write(f"- {col}: 列が存在しません")
        
        # 展示会名が空の場合、ファイル名から推測
        if '展示会名' in df.columns:
            df.loc[df['展示会名'] == '', '展示会名'] = inferred_event_name
        
        # 文字列列の前後空白除去
        str_cols = df.select_dtypes(include="object").columns
        df[str_cols] = df[str_cols].apply(lambda s: s.str.strip() if hasattr(s, 'str') else s)
        
        # 担当者名の補完（修正：「ご担当者」に変更）
        if "担当者" in df.columns:
            df["担当者"] = df["担当者"].fillna("").replace("", "ご担当者")
        
        # データ正規化
        if "Tel" in df.columns:
            df["Tel"] = df["Tel"].apply(normalize_phone)
        if "メールアドレス" in df.columns:
            df["メールアドレス"] = df["メールアドレス"].apply(validate_email)
        
        # 必須3列の検証を改善
        key_cols_check = ["メールアドレス", "Tel", "会社名"]
        validation_results = {}
        
        for col in key_cols_check:
            if col in df.columns:
                non_empty_data = df[df[col].notna() & (df[col] != '')]
                validation_results[col] = len(non_empty_data)
            else:
                validation_results[col] = 0
        
        # より詳細な検証
        st.write(f"🔍 **データ検証結果:**")
        for col, count in validation_results.items():
            st.write(f"- {col}: {count}件のデータあり")
        
        # 少なくとも1つの列にデータがあることを確認
        total_useful_data = sum(validation_results.values())
        if total_useful_data == 0:
            raise ValueError(f"必須項目（メールアドレス・Tel・会社名）にデータが全くありません。列名を確認してください。検出された列: {list(df.columns)}")
        
        # 会社名が全く空の場合のみエラー
        if validation_results.get("会社名", 0) == 0:
            raise ValueError("会社名のデータが全くありません。列名を確認してください。")

        # ファイル名と更新日時を追加
        df['ソースファイル'] = filename
        df['更新日時'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['処理月'] = datetime.now().strftime('%Y-%m')
        
        # 展示会名を設定（必須列として追加）
        if '展示会名' not in df.columns:
            df['展示会名'] = ""
        
        # 展示会名が空の場合、ファイル名から推測
        empty_exhibition_mask = (df['展示会名'].isna()) | (df['展示会名'] == '')
        df.loc[empty_exhibition_mask, '展示会名'] = inferred_event_name
        
        # 最終インデックスリセット（重要）
        df = df.reset_index(drop=True)
        
        st.success(f"✅ {filename}: {len(df)}行のデータを正常に処理しました")
        
        return df, stats, None
        
    except Exception as e:
        st.error(f"❌ {filename} の処理中にエラーが発生:")
        st.error(f"エラー詳細: {str(e)}")
        
        # デバッグ情報を表示
        if 'df' in locals():
            st.write(f"🔍 **デバッグ情報:**")
            st.write(f"- データフレームの形状: {df.shape}")
            st.write(f"- 列名: {list(df.columns)}")
            st.write(f"- データ型: {df.dtypes.to_dict()}")
        
        return None, {}, str(e)

def notion_download():
    """Notion APIからファイルダウンロード"""
    st.subheader("🔗 Notion APIからダウンロード")
    
    # API設定
    col1, col2 = st.columns(2)
    with col1:
        notion_api_key = st.text_input("Notion API Key", type="password", 
                                     value=os.environ.get("NOTION_API_KEY", DEFAULT_NOTION_API_KEY))
    with col2:
        database_id = st.text_input("Database ID", 
                                  value=os.environ.get("DATABASE_ID", DEFAULT_DATABASE_ID))
    
    # Google Sheets API設定
    st.markdown("### 🔑 Google Sheets API設定（非公開スプレッドシート用）")
    
    with st.expander("📋 Google Sheets API Key の取得方法"):
        st.markdown("""
        **1. Google Cloud Console にアクセス:**
        - https://console.cloud.google.com/
        
        **2. プロジェクトを作成または選択**
        
        **3. Google Sheets API を有効化:**
        - 「APIとサービス」→「ライブラリ」
        - 「Google Sheets API」を検索して有効化
        
        **4. 認証情報を作成:**
        - 「APIとサービス」→「認証情報」
        - 「認証情報を作成」→「APIキー」
        
        **5. APIキーを制限（推奨）:**
        - 「アプリケーションの制限」→「なし」
        - 「APIの制限」→「Google Sheets API」のみ
        """)
    
    col1, col2 = st.columns(2)
    with col1:
        google_api_key = st.text_input(
            "Google Sheets API Key (オプション)", 
            type="password",
            value=os.environ.get("GOOGLE_API_KEY", DEFAULT_GOOGLE_SHEETS_API_KEY),
            help="非公開スプレッドシートにアクセスするために必要"
        )
    with col2:
        fallback_option = st.selectbox(
            "アクセス失敗時の処理",
            ["スキップして続行", "エラーで停止"],
            help="非公開スプレッドシートにアクセスできない場合の動作"
        )
    
    # 処理方法の説明
    if google_api_key:
        st.info("🔑 **処理方法:** Google Sheets API → 公開URL → スキップ/エラー")
    else:
        st.warning("⚠️ **処理方法:** 公開URLのみ（非公開スプレッドシートは失敗します）")
    
    # フィルター分類選択
    st.markdown("### 🎯 データ分類")
    filter_option = st.radio(
        "取得データの分類方法",
        ["すべて統合", "カテゴリ別分類"],
        help="カテゴリ別分類：メールあり・TELあり・URLありで分けて取得"
    )
    
    if filter_option == "カテゴリ別分類":
        st.info("📂 以下の3つのカテゴリに分けて取得・処理します")
        
        # カテゴリ選択オプション
        st.markdown("### 📁 ダウンロードするカテゴリを選択")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            download_email = st.checkbox(
                "📧 メールありフォルダ",
                value=True,
                help="メールアドレスが「あり」のデータ"
            )
            if download_email:
                st.write("- あり")
        
        with col2:
            download_tel = st.checkbox(
                "📞 TELありフォルダ",
                value=True,
                help="電話番号情報があるデータ"
            )
            if download_tel:
                st.write("- TELとURL")
                st.write("- Tel、住所、URL")
        
        with col3:
            download_url = st.checkbox(
                "🌐 URLありフォルダ",
                value=True,
                help="URL情報があるデータ"
            )
            if download_url:
                st.write("- 社名・住所・URL")
                st.write("- 社名とURL（直で企業HPリンク）")
                st.write("- 社名とURLのみ")
        
        # 選択されたカテゴリを保存
        st.session_state.selected_categories = {
            "📧メールあり": download_email,
            "📞TELあり": download_tel,
            "🌐URLあり": download_url
        }
    
    # 更新モード選択
    st.markdown("### 📅 更新モード")
    col1, col2 = st.columns(2)
    with col1:
        update_mode = st.radio(
            "取得するファイル",
            ["今月の新規ファイルのみ", "期間を指定", "全ファイル（完全更新）"],
            help="月次運用では「今月の新規ファイルのみ」を推奨"
        )
    with col2:
        if update_mode == "今月の新規ファイルのみ":
            st.info("📅 最終更新日時が今月のファイルのみを取得します")
        elif update_mode == "期間を指定":
            st.info("📅 指定した期間のファイルを取得します")
        else:
            st.warning("⚠️ 全ファイルを取得します（時間がかかる場合があります）")
    
    # 期間指定の入力フィールド
    if update_mode == "期間を指定":
        st.markdown("### 📅 期間指定")
        col1, col2 = st.columns(2)
        with col1:
            from datetime import datetime, timedelta
            start_date = st.date_input(
                "開始日",
                value=datetime.now() - timedelta(days=30),
                help="取得する期間の開始日"
            )
        with col2:
            end_date = st.date_input(
                "終了日",
                value=datetime.now(),
                help="取得する期間の終了日"
            )
    
    if st.button("対象件数を確認", disabled=not (notion_api_key and database_id)):
        # カテゴリ別の場合、選択されていないカテゴリがあるか確認
        if filter_option == "カテゴリ別分類":
            selected_categories = st.session_state.get('selected_categories', {})
            selected_count = sum(selected_categories.values())
            
            if selected_count == 0:
                st.error("❌ 少なくとも1つのカテゴリを選択してください")
                return
            
            not_selected = [cat for cat, selected in selected_categories.items() if not selected]
            if not_selected:
                st.info(f"ℹ️ 以下のカテゴリはダウンロードされません: {', '.join(not_selected)}")
        
        try:
            notion = Client(
                auth=notion_api_key,
                notion_version="2022-06-28"
            )
            
            # フィルター条件を作成
            def create_filter_conditions(filter_option, update_mode, start_date=None, end_date=None):
                # 日付フィルター
                date_filter = None
                if update_mode == "今月の新規ファイルのみ":
                    from datetime import datetime
                    
                    now = datetime.now()
                    first_day = datetime(now.year, now.month, 1)
                    first_day_iso = first_day.isoformat() + "Z"
                    
                    # デバッグ用に期間を表示
                    st.info(f"🎯 フィルター期間: {now.strftime('%Y年%m月1日')} 00:00:00 以降")
                    st.code(f"API条件: last_edited_time >= {first_day_iso}", language="json")
                    
                    date_filter = {
                        "property": "Last edited time", 
                        "last_edited_time": {
                            "on_or_after": first_day_iso
                        }
                    }
                elif update_mode == "期間を指定" and start_date and end_date:
                    from datetime import datetime, time
                    
                    # 開始日の00:00:00
                    start_datetime = datetime.combine(start_date, time.min)
                    start_iso = start_datetime.isoformat() + "Z"
                    
                    # 終了日の23:59:59
                    end_datetime = datetime.combine(end_date, time.max)
                    end_iso = end_datetime.isoformat() + "Z"
                    
                    st.info(f"🎯 フィルター期間: {start_date.strftime('%Y年%m月%d日')} 〜 {end_date.strftime('%Y年%m月%d日')}")
                    st.code(f"API条件: {start_iso} <= last_edited_time <= {end_iso}", language="json")
                    
                    date_filter = {
                        "and": [
                            {
                                "property": "Last edited time",
                                "last_edited_time": {
                                    "on_or_after": start_iso
                                }
                            },
                            {
                                "property": "Last edited time",
                                "last_edited_time": {
                                    "on_or_before": end_iso
                                }
                            }
                        ]
                    }
                
                if filter_option == "すべて統合":
                    # 従来のすべて統合フィルター
                    base_filter = {
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
                    if date_filter:
                        base_filter["and"].append(date_filter)
                    
                    return {"すべて": base_filter}
                
                else:  # カテゴリ別分類
                    filters = {}
                    selected_categories = st.session_state.get('selected_categories', {
                        "📧メールあり": True,
                        "📞TELあり": True,
                        "🌐URLあり": True
                    })
                    
                    # 1. メールありフォルダ
                    if selected_categories.get("📧メールあり", True):
                        email_filter = {
                            "and": [
                                {"property": "メールアドレス（有無）", "select": {"equals": "あり"}},
                                {"property": "ファイル", "files": {"is_not_empty": True}}
                            ]
                        }
                        if date_filter:
                            email_filter["and"].append(date_filter)
                        filters["📧メールあり"] = email_filter
                    
                    # 2. TELありフォルダ
                    if selected_categories.get("📞TELあり", True):
                        tel_filter = {
                            "and": [
                                {
                                    "or": [
                                        {"property": "メールアドレス（有無）", "select": {"equals": "TELとURL"}},
                                        {"property": "メールアドレス（有無）", "select": {"equals": "Tel、住所、URL"}}
                                    ]
                                },
                                {"property": "ファイル", "files": {"is_not_empty": True}}
                            ]
                        }
                        if date_filter:
                            tel_filter["and"].append(date_filter)
                        filters["📞TELあり"] = tel_filter
                    
                    # 3. URLありフォルダ
                    if selected_categories.get("🌐URLあり", True):
                        url_filter = {
                            "and": [
                                {
                                    "or": [
                                        {"property": "メールアドレス（有無）", "select": {"equals": "社名・住所・URL"}},
                                        {"property": "メールアドレス（有無）", "select": {"equals": "社名とURL（直で企業HPリンク）"}},
                                        {"property": "メールアドレス（有無）", "select": {"equals": "社名とURLのみ"}}
                                    ]
                                },
                                {"property": "ファイル", "files": {"is_not_empty": True}}
                            ]
                        }
                        if date_filter:
                            url_filter["and"].append(date_filter)
                        filters["🌐URLあり"] = url_filter
                    
                    return filters
            
            # フィルター条件を取得
            if update_mode == "期間を指定":
                filter_conditions = create_filter_conditions(filter_option, update_mode, start_date, end_date)
            else:
                filter_conditions = create_filter_conditions(filter_option, update_mode)
            
            if update_mode == "今月の新規ファイルのみ":
                from datetime import datetime
                now = datetime.now()
                st.info(f"📅 フィルター条件: {now.strftime('%Y年%m月')}の最終更新日時")
                st.success("✅ 最終更新日時フィルターを使用（今月編集されたレコードのみ）")
            
            with st.spinner("対象件数を確認中..."):
                total_counts = {}
                
                for category, filter_condition in filter_conditions.items():
                    # 件数を取得
                    count = 0
                    start_cursor = None
                    
                    while True:
                        response = notion.databases.query(
                            database_id=database_id,
                            filter=filter_condition,
                            start_cursor=start_cursor,
                            page_size=100
                        )
                        items = response.get("results", [])
                        count += len(items)
                        
                        if not response.get("has_more"):
                            break
                        start_cursor = response.get("next_cursor")
                    
                    total_counts[category] = count
                
                # 結果表示
                if filter_option == "すべて統合":
                    total = total_counts["すべて"]
                    st.success(f"🎯 **対象アイテム: {total}件**")
                else:
                    st.success("🎯 **カテゴリ別対象件数:**")
                    col1, col2, col3 = st.columns(3)
                    
                    categories = list(total_counts.keys())
                    with col1:
                        if len(categories) > 0:
                            st.metric(categories[0], total_counts[categories[0]])
                    with col2:
                        if len(categories) > 1:
                            st.metric(categories[1], total_counts[categories[1]])
                    with col3:
                        if len(categories) > 2:
                            st.metric(categories[2], total_counts[categories[2]])
                    
                    total = sum(total_counts.values())
                    st.info(f"**合計: {total}件**")
                
                # 期間表示
                if update_mode == "今月の新規ファイルのみ":
                    st.info(f"📅 期間: {now.strftime('%Y年%m月')}の最終更新日時に該当するファイル")
                elif update_mode == "期間を指定":
                    st.info(f"📅 期間: {start_date.strftime('%Y年%m月%d日')} 〜 {end_date.strftime('%Y年%m月%d日')}")
                else:
                    st.info("📅 期間: 全期間")
                
                # セッションに保存
                st.session_state.target_counts = total_counts
                st.session_state.target_filters = filter_conditions
                st.session_state.target_mode = update_mode
                st.session_state.target_filter_option = filter_option
                st.session_state.google_api_key = google_api_key
                st.session_state.fallback_option = fallback_option
                
                if total > 0:
                    st.warning("⚠️ 対象件数を確認後、下の「ダウンロード実行」ボタンでファイルをダウンロードしてください")
                else:
                    st.error("❌ 対象となるアイテムが見つかりませんでした")
                
        except Exception as e:
            st.error(f"❌ エラーが発生しました: {e}")
    
    # 対象件数確認後のダウンロードボタン
    if 'target_counts' in st.session_state and sum(st.session_state.target_counts.values()) > 0:
        st.markdown("---")
        st.markdown("### 📥 ダウンロード実行")
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"🎯 **対象: {sum(st.session_state.target_counts.values())}件**")
            st.info(f"📅 **モード: {st.session_state.target_mode}**")
        
        with col2:
            if st.button("📥 ダウンロード実行", type="primary"):
                try:
                    notion = Client(
                        auth=notion_api_key,
                        notion_version="2022-06-28"
                    )
                    
                    # セッションから設定を取得
                    google_api_key = st.session_state.get('google_api_key', '')
                    fallback_option = st.session_state.get('fallback_option', 'スキップして続行')
                    
                    with st.spinner("Notionからファイルをダウンロード中..."):
                        # カテゴリ別処理
                        if st.session_state.target_filter_option == "カテゴリ別分類":
                            categorized_files = {}
                            
                            for category, filter_condition in st.session_state.target_filters.items():
                                all_items = []
                                start_cursor = None
                                
                                while True:
                                    response = notion.databases.query(
                                        database_id=database_id,
                                        filter=filter_condition,
                                        start_cursor=start_cursor
                                    )
                                    items = response.get("results", [])
                                    all_items.extend(items)
                                    if not response.get("has_more"):
                                        break
                                    start_cursor = response.get("next_cursor")
                                
                                st.info(f"{category}: {len(all_items)}件のアイテムを取得")
                                
                                # ファイルダウンロード処理
                                downloaded_files = []
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
                                                        success = False
                                                        
                                                        # 方法1: Google Sheets APIを試行
                                                        if google_api_key:
                                                            st.info(f"🔑 Google Sheets APIでアクセス中: {url}")
                                                            csv_data = google_sheet_to_csv_with_api(url, google_api_key)
                                                            if csv_data:
                                                                sheet_id = extract_sheet_id(url)
                                                                file_name = f"{page_title}_{sheet_id}_{item_idx+1}_{file_idx+1}.csv"
                                                                downloaded_files.append((file_name, csv_data))
                                                                success = True
                                                                st.success(f"✅ APIで取得成功: {file_name}")
                                                        
                                                        # 方法2: 公開URLを試行
                                                        if not success:
                                                            st.info(f"🌐 公開URLでアクセス中: {url}")
                                                            csv_url = google_sheet_to_csv_url(url)
                                                            if csv_url:
                                                                try:
                                                                    response = requests.get(csv_url, headers=headers)
                                                                    response.raise_for_status()
                                                                    sheet_id = extract_sheet_id(url)
                                                                    file_name = f"{page_title}_{sheet_id}_{item_idx+1}_{file_idx+1}.csv"
                                                                    downloaded_files.append((file_name, response.content))
                                                                    success = True
                                                                    st.success(f"✅ 公開URLで取得成功: {file_name}")
                                                                except requests.exceptions.HTTPError as e:
                                                                    if fallback_option == "スキップして続行":
                                                                        st.warning(f"⚠️ スキップ: {url} - {e}")
                                                                    else:
                                                                        raise e
                                                        
                                                        if not success and fallback_option == "エラーで停止":
                                                            raise Exception(f"スプレッドシートにアクセスできません: {url}")
                                                        elif not success:
                                                            st.warning(f"⚠️ アクセス失敗（スキップ）: {url}")
                                                        
                                            except Exception as e:
                                                if fallback_option == "スキップして続行":
                                                    st.warning(f"ファイル取得をスキップ: {str(e)}")
                                                else:
                                                    st.warning(f"ファイル取得エラー: {str(e)}")
                                
                                categorized_files[category] = downloaded_files
                            
                            st.session_state.notion_files_categorized = categorized_files
                            st.session_state.notion_update_mode = st.session_state.target_mode
                            
                            # 結果表示
                            st.success("✅ カテゴリ別ダウンロード完了")
                            for category, files in categorized_files.items():
                                st.info(f"{category}: {len(files)}個のファイル")
                        
                        else:  # すべて統合
                            # 保存されたフィルター条件を使用
                            all_items = []
                            start_cursor = None
                            
                            while True:
                                response = notion.databases.query(
                                    database_id=database_id,
                                    filter=st.session_state.target_filters["すべて"],
                                    start_cursor=start_cursor
                                )
                                items = response.get("results", [])
                                all_items.extend(items)
                                if not response.get("has_more"):
                                    break
                                start_cursor = response.get("next_cursor")
                            
                            st.success(f"🎯 {len(all_items)}件のアイテムを取得しました")
                            
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
                                                    success = False
                                                    
                                                    # 方法1: Google Sheets APIを試行
                                                    if google_api_key:
                                                        csv_data = google_sheet_to_csv_with_api(url, google_api_key)
                                                        if csv_data:
                                                            sheet_id = extract_sheet_id(url)
                                                            file_name = f"{page_title}_{sheet_id}_{item_idx+1}_{file_idx+1}.csv"
                                                            downloaded_files.append((file_name, csv_data))
                                                            success = True
                                                    
                                                    # 方法2: 公開URLを試行
                                                    if not success:
                                                        csv_url = google_sheet_to_csv_url(url)
                                                        if csv_url:
                                                            try:
                                                                response = requests.get(csv_url, headers=headers)
                                                                response.raise_for_status()
                                                                sheet_id = extract_sheet_id(url)
                                                                file_name = f"{page_title}_{sheet_id}_{item_idx+1}_{file_idx+1}.csv"
                                                                downloaded_files.append((file_name, response.content))
                                                                success = True
                                                            except requests.exceptions.HTTPError:
                                                                if fallback_option == "エラーで停止":
                                                                    raise
                                                    
                                                    if not success and fallback_option == "エラーで停止":
                                                        raise Exception(f"スプレッドシートにアクセスできません: {url}")
                                                    
                                        except Exception as e:
                                            if fallback_option == "スキップして続行":
                                                failed_files.append(f"スキップ: {str(e)}")
                                            else:
                                                failed_files.append(f"ファイル取得エラー: {str(e)}")
                                
                                progress_bar.progress((item_idx + 1) / len(all_items))
                            
                            st.session_state.notion_files = downloaded_files
                            st.session_state.notion_update_mode = st.session_state.target_mode
                            
                            # 結果表示
                            col1, col2 = st.columns(2)
                            with col1:
                                st.success(f"✅ {len(downloaded_files)}個のファイルをダウンロードしました")
                            with col2:
                                if failed_files:
                                    st.warning(f"⚠️ {len(failed_files)}個のファイルでエラーが発生")
                            
                            if failed_files:
                                with st.expander("❌ エラー詳細"):
                                    for error in failed_files:
                                        st.write(f"- {error}")
                        
                        # セッション状態をクリア
                        keys_to_delete = ['target_counts', 'target_filters', 'target_mode', 
                                        'target_filter_option', 'google_api_key', 'fallback_option']
                        for key in keys_to_delete:
                            if key in st.session_state:
                                del st.session_state[key]
                        
                except Exception as e:
                    st.error(f"❌ エラーが発生しました: {e}")

def file_upload_processing():
    """ファイルアップロード処理"""
    st.subheader("📁 ファイルアップロード・統合処理")
    
    # Notionからダウンロードしたファイルがある場合
    if 'notion_files' in st.session_state and st.session_state.notion_files:
        update_mode = st.session_state.get('notion_update_mode', '不明')
        st.info(f"💾 Notionから{len(st.session_state.notion_files)}個のファイルがダウンロード済み（{update_mode}）")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📥 Notionファイルを処理"):
                process_files(st.session_state.notion_files, "Notion")
        with col2:
            merge_option = st.checkbox(
                "既存データと統合", 
                value=True, 
                help="チェック時：既存データと統合し重複削除\n未チェック時：新規データで完全置換"
            )
            st.session_state.merge_with_existing = merge_option
        with col3:
            if st.button("🗑️ Notionファイルをクリア"):
                del st.session_state.notion_files
                if 'notion_update_mode' in st.session_state:
                    del st.session_state.notion_update_mode
                st.rerun()
    
    # カテゴリ別ダウンロードファイルがある場合
    if 'notion_files_categorized' in st.session_state and st.session_state.notion_files_categorized:
        st.info("💾 カテゴリ別ファイルがダウンロード済みです")
        
        for category, files in st.session_state.notion_files_categorized.items():
            if files:
                st.markdown(f"#### {category} ({len(files)}個)")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button(f"📥 {category}を処理", key=f"process_{category}"):
                        process_files(files, f"{category}")
                
                with col2:
                    merge_option = st.checkbox(
                        "既存データと統合", 
                        value=True, 
                        key=f"merge_{category}",
                        help="チェック時：既存データと統合し重複削除\n未チェック時：新規データで完全置換"
                    )
                    st.session_state[f'merge_with_existing_{category}'] = merge_option
                
                with col3:
                    if st.button(f"🗑️ {category}をクリア", key=f"clear_{category}"):
                        del st.session_state.notion_files_categorized[category]
                        st.rerun()
        
        # 全カテゴリクリア
        if st.button("🗑️ 全カテゴリをクリア"):
            del st.session_state.notion_files_categorized
            if 'notion_update_mode' in st.session_state:
                del st.session_state.notion_update_mode
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
    """ファイルデータを処理（軽量化版）"""
    processed_dfs = []
    error_files = []
    total_stats = {"email_extracted": 0, "tel_extracted": 0, "files_processed": 0}
    
    # 大量処理用の設定
    total_files = len(file_data)
    batch_size = 25  # 25ファイルごとにバッチ処理（メモリ効率改善）
    is_large_batch = total_files > 20
    
    # 700件以上の大量処理対応
    if total_files > 700:
        batch_size = 15  # さらに小さいバッチに
        st.warning(f"""
        ⚠️ 大量ファイル処理モード（{total_files}件）
        - 処理に時間がかかります
        - メモリ使用量を最適化します
        - 途中経過が表示されます
        """)
    
    # プログレスバーとステータス
    progress_bar = st.progress(0)
    status_container = st.empty()
    
    if is_large_batch:
        st.info(f"🔄 大量処理モード：{total_files}件のファイルを処理中...")
        debug_mode = st.checkbox("詳細デバッグを表示", value=False, 
                                help="大量処理時は無効を推奨（処理速度向上）")
    else:
        debug_mode = True
    
    # バッチ処理
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch_files = file_data[batch_start:batch_end]
        
        if is_large_batch:
            status_container.info(f"📦 バッチ {batch_start//batch_size + 1}/{(total_files-1)//batch_size + 1} 処理中 ({batch_start+1}-{batch_end}件目)")
        
        batch_processed = []
        batch_errors = []
        
        for idx, (filename, content) in enumerate(batch_files):
            global_idx = batch_start + idx
            
            if not is_large_batch or debug_mode:
                status_container.text(f"処理中: {filename} ({global_idx+1}/{total_files})")
            
            try:
                # 軽量化されたファイル読み込み
                df = process_single_file_lightweight(filename, content, debug_mode)
                
                if df is not None:
                    # 軽量化されたデータ処理
                    processed_df, stats, error = process_dataframe_lightweight(df, filename, debug_mode)
                    
                    if processed_df is not None:
                        batch_processed.append(processed_df)
                        total_stats["email_extracted"] += stats["email_extracted"]
                        total_stats["tel_extracted"] += stats["tel_extracted"]
                        total_stats["files_processed"] += 1
                        
                        # 成功ログ（簡潔）
                        if not is_large_batch or debug_mode:
                            st.success(f"✅ {filename}: {len(processed_df)}行処理完了")
                    else:
                        batch_errors.append((filename, error))
                        if not is_large_batch or debug_mode:
                            st.error(f"❌ {filename}: {error}")
                else:
                    batch_errors.append((filename, "ファイル読み込み失敗"))
                    
            except Exception as e:
                batch_errors.append((filename, str(e)))
                if not is_large_batch or debug_mode:
                    st.error(f"❌ {filename}: {e}")
            
            # プログレスバー更新
            progress_bar.progress((global_idx + 1) / total_files)
        
        # バッチ結果をメインリストに追加
        processed_dfs.extend(batch_processed)
        error_files.extend(batch_errors)
        
        # 大量処理時のメモリクリア
        if is_large_batch and len(processed_dfs) > 100:
            import gc
            gc.collect()
        
        # バッチ完了通知
        if is_large_batch:
            success_count = len(batch_processed)
            error_count = len(batch_errors)
            st.info(f"📦 バッチ {batch_start//batch_size + 1} 完了: 成功{success_count}件, エラー{error_count}件")
    
    # データ統合（軽量化・安全化）
    if processed_dfs:
        status_container.info("🔗 データ統合中...")
        
        # Step 1: 各DataFrameの前処理（安全化）
        safe_dfs = []
        for i, df in enumerate(processed_dfs):
            try:
                # インデックスを明示的にリセット
                df = df.reset_index(drop=True)
                
                # 列名の重複チェック・修正
                columns = df.columns.tolist()
                seen_columns = {}
                new_columns = []
                
                for col in columns:
                    col_str = str(col)
                    if col_str in seen_columns:
                        seen_columns[col_str] += 1
                        new_col_name = f"{col_str}_dup{seen_columns[col_str]}"
                        new_columns.append(new_col_name)
                        if debug_mode:
                            st.warning(f"⚠️ DataFrame{i}: 重複列名修正 '{col_str}' → '{new_col_name}'")
                    else:
                        seen_columns[col_str] = 0
                        new_columns.append(col_str)
                
                df.columns = new_columns
                
                # 列名を文字列に統一
                df.columns = [str(col) for col in df.columns]
                
                safe_dfs.append(df)
                
            except Exception as e:
                if debug_mode:
                    st.error(f"❌ DataFrame{i}の前処理でエラー: {e}")
                continue
        
        if not safe_dfs:
            st.error("❌ 有効なDataFrameがありません")
            return
        
        # Step 2: 安全な結合処理
        try:
            # メモリ効率的な統合
            if len(safe_dfs) > 100:
                # 大量データの場合、段階的に統合
                st.info("📊 大量データを段階的に統合中...")
                # ファイル数に応じたチャンクサイズ調整
                if len(safe_dfs) > 700:
                    chunk_size = 15  # 大量ファイル時は小さいチャンク
                elif len(safe_dfs) > 300:
                    chunk_size = 25
                else:
                    chunk_size = 50
                
                merged_chunks = []
                
                for i in range(0, len(safe_dfs), chunk_size):
                    chunk = safe_dfs[i:i+chunk_size]
                    
                    # safe_concat_dataframes関数を使用
                    merged_chunk = safe_concat_dataframes(chunk, debug_mode)
                    
                    merged_chunks.append(merged_chunk)
                    
                    if i % (chunk_size * 5) == 0:  # 5チャンクごとに進捗表示
                        progress_pct = min((i+chunk_size) / len(safe_dfs) * 100, 100)
                        st.info(f"統合進捗: {min(i+chunk_size, len(safe_dfs))}/{len(safe_dfs)} チャンク ({progress_pct:.1f}%)")
                        
                        # メモリクリア（700件以上の場合）
                        if len(safe_dfs) > 700 and i % (chunk_size * 10) == 0:
                            import gc
                            gc.collect()
                
                # 最終統合も同様に修正
                if len(merged_chunks) == 1:
                    merged_df = merged_chunks[0]
                else:
                    aligned_chunks = align_dataframe_columns(merged_chunks, debug_mode)
                    merged_df = safe_concat_dataframes(aligned_chunks, debug_mode)
            else:
                # 通常データの場合
                if len(safe_dfs) == 1:
                    merged_df = safe_dfs[0].copy()
                else:
                    # 列名を統一してから結合
                    aligned_dfs = align_dataframe_columns(safe_dfs, debug_mode)
                    merged_df = safe_concat_dataframes(aligned_dfs, debug_mode)
            
            # 最終インデックスリセット
            merged_df = merged_df.reset_index(drop=True)
            
        except Exception as e:
            st.error(f"❌ データ統合エラー: {e}")
            st.info("🔄 代替方法で統合を試行中...")
            
            # 代替統合方法
            try:
                merged_df = concatenate_dataframes_safely(safe_dfs, debug_mode)
            except Exception as e2:
                st.error(f"❌ 代替統合も失敗: {e2}")
                return
        
        # 軽量化された重複削除
        merged_df = remove_duplicates_lightweight(merged_df, is_large_batch)
        
        # 既存データとの統合
        if not st.session_state.merged_data.empty and st.session_state.get('merge_with_existing', True):
            status_container.info("🔄 既存データと統合中...")
            combined_df = pd.concat([st.session_state.merged_data, merged_df], ignore_index=True)
            combined_df = remove_duplicates_lightweight(combined_df, True)
            st.session_state.merged_data = combined_df
        else:
            st.session_state.merged_data = merged_df
        
        # 統計情報保存
        st.session_state.processing_stats = total_stats
        
        # 簡潔な結果表示
        st.success(f"""
        ✅ **{source_type}ファイル処理完了**
        - 処理ファイル数: {total_stats['files_processed']}/{total_files}個
        - 最終データ数: {len(st.session_state.merged_data)}件
        - エラーファイル数: {len(error_files)}個
        """)
    
    # エラーファイル表示（簡潔）
    if error_files:
        with st.expander(f"❌ エラーファイル一覧 ({len(error_files)}件)"):
            for filename, error in error_files:
                st.write(f"- **{filename}**: {error}")
    
    status_container.empty()
    progress_bar.empty()

def process_single_file_lightweight(filename, content, debug_mode=False):
    """軽量化されたファイル読み込み"""
    try:
        if filename.lower().endswith('.csv'):
            # 日本語CSV用の軽量エンコーディング検出
            encodings_to_try = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
            detected_encoding = detect(content).get("encoding", "utf-8")
            
            if detected_encoding and detected_encoding not in encodings_to_try:
                encodings_to_try.insert(0, detected_encoding)
            
            for encoding in encodings_to_try:
                try:
                    content_str = content.decode(encoding)
                    if content_str.startswith('\ufeff'):
                        content_str = content_str[1:]
                    
                    df = pd.read_csv(io.StringIO(content_str), dtype=str, on_bad_lines="skip")
                    df = df.reset_index(drop=True)  # 追加：インデックスリセット
                    
                    # 簡単な文字化けチェック
                    first_col = str(df.columns[0]) if len(df.columns) > 0 else ""
                    if not any(suspect in first_col for suspect in ['録音', '墨訂', '震災']):
                        if debug_mode:
                            st.info(f"✅ {encoding}で読み込み成功: {filename}")
                        return df
                except:
                    continue
            
            # 全て失敗した場合、強制デコード
            content_str = content.decode(detected_encoding, errors='ignore')
            df = pd.read_csv(io.StringIO(content_str), dtype=str, on_bad_lines="skip")
            df = df.reset_index(drop=True)  # 追加：インデックスリセット
            
            # 文字化け自動修正
            if any(any(suspect in str(col) for suspect in ['録音', '墨訂', '震災']) for col in df.columns):
                expected_columns = ['展示会名', '業種', '展示会初日', '展示会最終日', '会社名タイトル', '住所', 'TEL', 'URL', '会社名']
                if len(df.columns) == len(expected_columns):
                    df.columns = expected_columns
                    df = df.reset_index(drop=True)  # 追加：列名変更後もリセット
                    if debug_mode:
                        st.warning(f"⚠️ 文字化け修正: {filename}")
            
            return df
            
        else:
            # Excelファイル
            df = pd.read_excel(io.BytesIO(content), dtype=str, engine='openpyxl')
            return df.reset_index(drop=True)  # 追加：インデックスリセット
            
    except Exception as e:
        if debug_mode:
            st.error(f"❌ ファイル読み込みエラー: {filename} - {e}")
        return None

def process_dataframe_lightweight(df, filename, debug_mode=False):
    """軽量化されたデータフレーム処理"""
    try:
        # 大量データ対応：メモリ効率化
        if len(df) > 10000:
            # 大きいデータフレームの場合、不要な列を削除
            df = df.copy()
            empty_cols = [col for col in df.columns if df[col].isna().all()]
            if empty_cols:
                df = df.drop(columns=empty_cols)
        
        stats = {"email_extracted": 0, "tel_extracted": 0}
        inferred_event_name = os.path.splitext(filename)[0]
        
        # インデックスを明示的にリセット（重要）
        df = df.reset_index(drop=True)
        
        # 重複列名修正（簡潔版）
        original_columns = list(df.columns)
        seen_columns = {}
        new_columns = []
        
        for col in original_columns:
            col_str = str(col).strip()
            if col_str in seen_columns:
                seen_columns[col_str] += 1
                new_col_name = f"{col_str}_dup{seen_columns[col_str]}"
                new_columns.append(new_col_name)
                if debug_mode:
                    st.warning(f"⚠️ 重複列名修正: '{col_str}' → '{new_col_name}'")
            else:
                seen_columns[col_str] = 0
                new_columns.append(col_str)
        
        df.columns = new_columns
        
        # 列名を文字列に統一
        df.columns = [str(col) for col in df.columns]
        
        # 再度インデックスリセット
        df = df.reset_index(drop=True)
        
        # 不要な列を削除
        columns_to_drop = []
        for col in df.columns:
            col_lower = str(col).lower().strip()
            # ブース番号、小間番号、ロゴURLを削除
            if any(keyword in col_lower for keyword in ['ブース番号', '小間番号', 'ロゴurl', 'ロゴ url', 'logo', 'booth', '小間', 'ブース']):
                columns_to_drop.append(col)
                if debug_mode:
                    st.info(f"🗑️ 不要列削除: {col}")
        
        if columns_to_drop:
            df = df.drop(columns=columns_to_drop)
        
        # 列名正規化（簡潔版）
        rename_dict = {}
        for col in df.columns:
            col_clean = str(col).strip()
            col_lower = col_clean.lower()
            
            # 完全一致チェック
            for old_name, new_name in COLUMN_RENAMES.items():
                if col_lower == old_name.lower():
                    rename_dict[col] = new_name
                    break
            
            # 部分マッチング（最小限）
            if col not in rename_dict:
                if 'tel' in col_lower and 'url' not in col_lower:
                    rename_dict[col] = 'Tel'
                elif 'mail' in col_lower and 'url' not in col_lower:
                    rename_dict[col] = 'メールアドレス'
                elif any(kw in col_clean for kw in ['会社', '企業']) and 'url' not in col_lower:
                    rename_dict[col] = '会社名'
                elif col_lower in ['url', 'website'] and len(col_clean) < 15:
                    rename_dict[col] = 'Website'
                elif any(kw in col_clean for kw in ['展示会初日', '開始日', '初日', '開催日']) and 'url' not in col_lower:
                    rename_dict[col] = '展示会初日'
        
        # 安全な列名変換
        try:
            df.rename(columns=rename_dict, inplace=True)
            # 再度列名を文字列に統一
            df.columns = [str(col) for col in df.columns]
        except Exception as e:
            if debug_mode:
                st.warning(f"⚠️ 列名変換エラー: {e}")
        
        # 必須列追加
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        
        # 展示会初日列も追加
        if '展示会初日' not in df.columns:
            df['展示会初日'] = ""
        
        # 展示会名設定
        if '展示会名' in df.columns:
            empty_mask = df['展示会名'].isna() | (df['展示会名'] == '')
            df.loc[empty_mask, '展示会名'] = inferred_event_name
        
        # データ正規化（最小限）
        try:
            if "Tel" in df.columns:
                df["Tel"] = df["Tel"].apply(normalize_phone)
            if "メールアドレス" in df.columns:
                df["メールアドレス"] = df["メールアドレス"].apply(validate_email)
        except Exception as e:
            if debug_mode:
                st.warning(f"⚠️ データ正規化エラー: {e}")
        
        # 担当者補完（修正：「ご担当者」に変更）
        if "担当者" in df.columns:
            df["担当者"] = df["担当者"].fillna("ご担当者").replace("", "ご担当者")
        
        # メタデータ追加
        df['ソースファイル'] = filename
        df['更新日時'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['処理月'] = datetime.now().strftime('%Y-%m')
        
        # 最終インデックスリセット
        df = df.reset_index(drop=True)
        
        return df, stats, None
        
    except Exception as e:
        return None, {}, str(e)

def align_dataframe_columns(dfs, debug_mode=False):
    """DataFrameの列名を統一して安全に結合準備"""
    if not dfs:
        return []
    
    # 全DataFrameの列名を収集
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    
    all_columns = sorted(list(all_columns))
    
    if debug_mode:
        st.info(f"🔧 列名統一: {len(all_columns)}個の列を統一")
    
    # 各DataFrameを同じ列構成に統一
    aligned_dfs = []
    for i, df in enumerate(dfs):
        try:
            # 欠けている列を追加
            for col in all_columns:
                if col not in df.columns:
                    df[col] = ""
            
            # 列順序を統一
            df = df[all_columns]
            
            # インデックスリセット
            df = df.reset_index(drop=True)
            
            aligned_dfs.append(df)
            
        except Exception as e:
            if debug_mode:
                st.warning(f"⚠️ DataFrame{i}のアライメントエラー: {e}")
            continue
    
    return aligned_dfs

def concatenate_dataframes_safely(dfs, debug_mode=False):
    """安全なDataFrame結合（代替方法）"""
    if not dfs:
        return pd.DataFrame()
    
    if len(dfs) == 1:
        return dfs[0].reset_index(drop=True)
    
    # 最初のDataFrameをベースにする
    result_df = dfs[0].copy().reset_index(drop=True)
    base_columns = list(result_df.columns)
    
    for i, df in enumerate(dfs[1:], 1):
        try:
            df = df.reset_index(drop=True)
            df_columns = list(df.columns)
            
            # 共通列のみを保持
            common_columns = [col for col in base_columns if col in df_columns]
            
            # 新しい列を追加
            new_columns = [col for col in df_columns if col not in base_columns]
            
            if new_columns and debug_mode:
                st.info(f"DataFrame{i}: 新規列追加 {new_columns}")
            
            # ベースDataFrameに新しい列を追加
            for col in new_columns:
                result_df[col] = ""
                base_columns.append(col)
            
            # 結合するDataFrameに欠けている列を追加
            for col in base_columns:
                if col not in df.columns:
                    df[col] = ""
            
            # 列順序を統一
            df = df[base_columns]
            
            # 行を追加
            result_df = pd.concat([result_df, df], ignore_index=True, sort=False)
            result_df = result_df.reset_index(drop=True)
            
        except Exception as e:
            if debug_mode:
                st.warning(f"⚠️ DataFrame{i}の結合エラー: {e} - スキップします")
            continue
    
    return result_df

def remove_duplicates_lightweight(df, is_large=False):
    """軽量化された重複削除（日付ベース、メールアドレス考慮）"""
    if is_large:
        st.info("🔄 重複削除中...")
    
    # インデックスリセット
    df = df.reset_index(drop=True)
    
    original_count = len(df)
    
    try:
        # 会社名+展示会名+メールアドレスでの重複削除（日付ベース）
        if '会社名' in df.columns and '展示会名' in df.columns:
            # 日付列を datetime 型に変換
            date_cols_parsed = []
            
            # 展示会初日の変換を試みる
            if '展示会初日' in df.columns:
                try:
                    df['_parsed_exhibition_date'] = pd.to_datetime(df['展示会初日'], errors='coerce')
                    date_cols_parsed.append('_parsed_exhibition_date')
                except:
                    pass
            
            # 最終更新日の変換を試みる（もし存在すれば）
            if '最終更新日' in df.columns:
                try:
                    df['_parsed_update_date'] = pd.to_datetime(df['最終更新日'], errors='coerce')
                    date_cols_parsed.append('_parsed_update_date')
                except:
                    pass
            
            # 日付でソート（新しい順）
            if date_cols_parsed:
                # 複数の日付列がある場合は、最も新しい日付を使用
                if len(date_cols_parsed) > 1:
                    df['_max_date'] = df[date_cols_parsed].max(axis=1)
                    sort_col = '_max_date'
                else:
                    sort_col = date_cols_parsed[0]
                
                # 日付でソート（新しい順、NaTは最後に）
                df = df.sort_values(by=sort_col, ascending=False, na_position='last')
            
            # 重複削除の基準を設定
            # メールアドレス列が存在する場合は、それも重複判定に含める
            if 'メールアドレス' in df.columns:
                # メールアドレスを正規化（空文字とNaNを統一）
                df['_normalized_email'] = df['メールアドレス'].fillna('').str.strip()
                df.loc[df['_normalized_email'] == '', '_normalized_email'] = pd.NA
                
                # 会社名 + 展示会名 + メールアドレスで重複削除
                duplicate_subset = ['会社名', '展示会名', '_normalized_email']
            else:
                duplicate_subset = ['会社名', '展示会名']
            
            # 重複削除（最初のもの=最新を残す）
            df = df.drop_duplicates(subset=duplicate_subset, keep='first')
            
            # 一時的な列を削除
            temp_cols = ['_parsed_exhibition_date', '_parsed_update_date', '_max_date', '_normalized_email']
            for col in temp_cols:
                if col in df.columns:
                    df = df.drop(columns=[col])
            
            df = df.reset_index(drop=True)
        
        removed_count = original_count - len(df)
        if is_large and removed_count > 0:
            st.info(f"🗑️ {removed_count}件の重複行を削除（メールアドレスが異なる場合は別データとして保持）")
    
    except Exception as e:
        st.warning(f"⚠️ 重複削除中にエラー: {e}")
    
    return df

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
        
        # ダウンロードオプション
        with st.expander("⚙️ ダウンロードオプション", expanded=True):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                download_email_only = st.checkbox(
                    "メールアドレスありのみ",
                    help="メールアドレスが入力されているデータのみダウンロード"
                )
            
            with col2:
                # 業界カテゴリ選択
                if '業界' in filtered_data.columns:
                    unique_industries = filtered_data['業界'].dropna().unique().tolist()
                    selected_industries = st.multiselect(
                        "業界を選択",
                        options=unique_industries,
                        default=unique_industries,
                        help="ダウンロードする業界を選択"
                    )
            
            with col3:
                # 展示会選択
                if '展示会名' in filtered_data.columns:
                    unique_exhibitions = filtered_data['展示会名'].dropna().unique().tolist()
                    selected_exhibitions = st.multiselect(
                        "展示会を選択",
                        options=unique_exhibitions,
                        default=unique_exhibitions,
                        help="ダウンロードする展示会を選択"
                    )
        
        # フィルタリング後のデータ
        download_data = filtered_data.copy()
        
        # メールアドレスありのみフィルタ
        if download_email_only:
            download_data = download_data[
                (download_data['メールアドレス'].notna()) & 
                (download_data['メールアドレス'] != '')
            ]
        
        # 業界フィルタ
        if '業界' in download_data.columns and 'selected_industries' in locals():
            download_data = download_data[download_data['業界'].isin(selected_industries)]
        
        # 展示会フィルタ
        if '展示会名' in download_data.columns and 'selected_exhibitions' in locals():
            download_data = download_data[download_data['展示会名'].isin(selected_exhibitions)]
        
        st.info(f"🎯 ダウンロード対象: {len(download_data)}件")
        
        # SNS列の統合処理を実行
        download_data = merge_sns_columns(download_data)
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            csv = download_data.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📄 CSVダウンロード",
                data=csv,
                file_name=f"exhibition_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col2:
            # Excelダウンロード
            try:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    download_data.to_excel(writer, index=False, sheet_name='ExhibitionData')
                excel_data = output.getvalue()
                
                st.download_button(
                    label="📊 Excelダウンロード",
                    data=excel_data,
                    file_name=f"exhibition_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            except ImportError:
                st.error("⚠️ Excelダウンロードにはopenpyxlが必要です。\n`pip install openpyxl`を実行してください。")
        
        with col3:
            # メールリストのみ（修正：展示会初日を追加）
            email_columns = ['会社名', '担当者', 'メールアドレス', '展示会名', '業界']
            if '展示会初日' in filtered_data.columns:
                email_columns.append('展示会初日')
            
            # ダウンロードデータが空の場合はフィルター前のデータを使用
            email_source = download_data if not download_data.empty else filtered_data
            email_only = email_source[
                (email_source['メールアドレス'].notna()) & 
                (email_source['メールアドレス'] != '')
            ]
            # 必要な列のみ選択（存在する列のみ）
            available_email_cols = [col for col in email_columns if col in email_only.columns]
            email_only = email_only[available_email_cols] if available_email_cols else email_only
            
            email_csv = email_only.to_csv(index=False, encoding='utf-8-sig')
            st.download_button(
                label="📧 メールリスト",
                data=email_csv,
                file_name=f"email_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        
        with col4:
            # テレアポリスト（修正：展示会初日を追加）
            tel_columns = ['会社名', '担当者', 'Tel', '展示会名', '業界']
            if '展示会初日' in filtered_data.columns:
                tel_columns.append('展示会初日')
            
            # ダウンロードデータが空の場合はフィルター前のデータを使用
            tel_source = download_data if not download_data.empty else filtered_data
            tel_only = tel_source[
                (tel_source['Tel'].notna()) & 
                (tel_source['Tel'] != '')
            ]
            # 必要な列のみ選択（存在する列のみ）
            available_tel_cols = [col for col in tel_columns if col in tel_only.columns]
            tel_only = tel_only[available_tel_cols] if available_tel_cols else tel_only
            
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
    st.sidebar.title("📌 機能選択")
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