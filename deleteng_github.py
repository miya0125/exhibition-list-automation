"""Automated NG filtering driven by two Google Sheets templates.

This version replaces the interactive desktop workflow so it can run unattended
(e.g. on GitHub Actions). All runtime instructions live in Google Sheets that
are originally created from two CSV templates:

1. 指示書 (instructions_template.csv)
   - 設定項目と値の2列テーブル。
   - 主なキー: 入力/出力スプレッドシートID・シート名、出力ファイル名、使用するNGタブ、
     業界NGキーワード、解除URLベースなど。
2. NGリスト (ng_list_template.csv)
   - 1タブにつき1つのリストを作成し、必要に応じてタブをコピーして共通/クライアント別/
     フリーメールドメインなどを管理。
   - 列は「種別」「値」「使用」「備考」を想定。種別はプルダウンで「会社名」または
     「メールアドレス・ドメイン」。
   - 会社名で部分一致にしたい場合は `*キーワード*`（全角＊でも可）で囲む。

必須環境変数:
* GOOGLE_SERVICE_ACCOUNT_JSON（または *_INFO 等の別名）: サービスアカウント JSON
* CONFIG_SPREADSHEET_ID: 指示書を配置したスプレッドシートの ID
* CONFIG_WORKSHEET (任意): 指示書タブ名。未指定時は「指示書」

実行フロー:
1. 指示書タブから入出力スプレッドシートや NG タブ名、業界 NG キーワードを読み込み。
2. 指定された NG タブを順に読み込み、会社名・メール/ドメイン NG を集約。
3. 入力シートを取得し、トリム → NG 判定 → 重複カウント列 → 登録解除 URL 作成。
4. 結果を出力シートへ上書きし、必要なら `出力ファイル名` でローカル Excel も生成。
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:
    import gspread
except ImportError as exc:  # pragma: no cover - surfaced at runtime
    raise SystemExit(
        "gspread is required. Install with `pip install gspread google-auth`"
    ) from exc

from google.oauth2 import service_account

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
DEFAULT_CONFIG_SHEET = "指示書"
DEFAULT_TRIM_COLUMNS = [
    "メールアドレス",
    "メールアドレス(テキスト)",
    "展示会名",
    "担当者",
    "業界",
    "業種",
    "会社名",
    "会社名(テキスト)",
]
DEFAULT_EMAIL_CANDIDATES = [
    "メールアドレス(テキスト)",
    "メールアドレス",
    "e-mail",
    "email",
    "mail",
]
DEFAULT_COMPANY_CANDIDATES = [
    "会社名(テキスト)",
    "会社名",
    "企業名",
    "社名",
    "company",
]
DEFAULT_INDUSTRY_CANDIDATES = [
    "業界",
    "業種",
    "industry",
    "sector",
    "category",
]
LEGAL_TOKENS = [
    "株式会社",
    "（株）",
    "(株)",
    "株)",
    "㈱",
    "有限会社",
    "（有）",
    "(有)",
    "㈲",
    "合名会社",
    "合資会社",
    "合同会社",
    "一般社団法人",
    "一般財団法人",
    "公益社団法人",
    "公益財団法人",
    "学校法人",
    "医療法人",
    "特定非営利活動法人",
    "npo法人",
    "npo",
    "inc.",
    "inc",
    "co., ltd.",
    "co.,ltd.",
    "co ltd",
    "co., limited",
    "llc",
    "g.k.",
    "gk",
    "有限責任事業組合",
]
LEGAL_RE = re.compile(
    "|".join(map(re.escape, sorted(LEGAL_TOKENS, key=len, reverse=True))),
    re.IGNORECASE,
)

INSTRUCTION_KEY_ALIASES = {
    "入力スプレッドシートid": "input_spreadsheet_id",
    "inputspreadsheetsid": "input_spreadsheet_id",
    "入力シート名": "input_worksheet",
    "入力タブ": "input_worksheet",
    "入力ページ": "input_worksheet",
    "出力スプレッドシートid": "output_spreadsheet_id",
    "outputspreadsheetsid": "output_spreadsheet_id",
    "出力シート名": "output_worksheet",
    "出力タブ": "output_worksheet",
    "出力ページ": "output_worksheet",
    "出力ファイル名": "output_filename",
    "出力ファイル": "output_filename",
    "ngリストスプレッドシートid": "ng_spreadsheet_id",
    "ngスプレッドシートid": "ng_spreadsheet_id",
    "ngスプレッドシート": "ng_spreadsheet_id",
    "ng使用タブ": "ng_tabs",
    "使用するngタブ": "ng_tabs",
    "ngタブ": "ng_tabs",
    "業界ngキーワード": "industry_keywords",
    "業界キーワード": "industry_keywords",
    "解除urlベース": "unsubscribe_base_url",
    "解除url": "unsubscribe_base_url",
    "トリム対象列": "trim_columns",
    "メール列候補": "email_column_candidates",
    "会社列候補": "company_column_candidates",
    "業界列候補": "industry_column_candidates",
}
KEY_COLUMN_ALIASES = {"項目", "item", "key", "設定", "name"}
VALUE_COLUMN_ALIASES = {"値", "value", "内容", "設定値"}
CATEGORY_COLUMN_ALIASES = {"種別", "分類", "type", "カテゴリ"}
NG_VALUE_COLUMN_ALIASES = {"値", "value", "ng", "対象", "項目"}
ENABLED_COLUMN_ALIASES = {"使用", "enabled", "use", "有効"}

TYPE_COMPANY_TOKENS = {"会社名", "企業", "company"}
TYPE_EMAIL_TOKENS = {"メールアドレス", "メールアドレス・ドメイン", "メール", "ドメイン", "email", "mail"}
CONTAINS_MARKERS = {"*", "＊"}


class ConfigError(RuntimeError):
    """Raised when the instruction sheet is missing required information."""


@dataclass
class Config:
    raw: Dict[str, str]
    config_spreadsheet_id: str
    config_worksheet: str

    def require(self, key: str) -> str:
        val = (self.raw.get(key) or "").strip()
        if not val:
            raise ConfigError(
                f"Config key '{key}' is required in worksheet "
                f"'{self.config_worksheet}' ({self.config_spreadsheet_id})."
            )
        return val

    def optional(self, key: str, default: Optional[str] = None) -> Optional[str]:
        val = (self.raw.get(key) or "").strip()
        return val if val else default


def normalize_identifier(text: str) -> str:
    if text is None:
        return ""
    normalized = unicodedata.normalize("NFKC", text)
    normalized = re.sub(r"[\s　:_\-]+", "", normalized)
    return normalized.lower()


def authorize_from_env() -> gspread.Client:
    """Authorize a gspread client using service-account JSON from env vars."""

    candidates = [
        "GOOGLE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_SERVICE_ACCOUNT_INFO",
        "GCP_SERVICE_ACCOUNT_JSON",
        "SERVICE_ACCOUNT_JSON",
    ]
    info: Optional[Dict[str, object]] = None
    for env_name in candidates:
        raw = os.environ.get(env_name)
        if not raw:
            continue
        raw = raw.strip()
        if not raw:
            continue
        if raw.startswith("{"):
            try:
                info = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise SystemExit(
                    f"Environment variable {env_name} contains invalid JSON"
                ) from exc
        else:
            try:
                with open(raw, "r", encoding="utf-8") as f:
                    info = json.load(f)
            except FileNotFoundError as exc:
                raise SystemExit(
                    f"Service-account file not found at {raw} (from {env_name})"
                ) from exc
        if info:
            break
    if not info:
        raise SystemExit(
            "Service-account JSON is required. Provide via GOOGLE_SERVICE_ACCOUNT_JSON"
        )

    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def load_config(client: gspread.Client, spreadsheet_id: str, worksheet: str) -> Config:
    ss = client.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet(worksheet)
    except gspread.WorksheetNotFound as exc:
        raise ConfigError(
            f"Instruction worksheet '{worksheet}' not found in spreadsheet {spreadsheet_id}."
        ) from exc

    df = worksheet_to_dataframe(ws)
    if df.empty:
        raise ConfigError(
            f"Instruction worksheet '{worksheet}' is empty. Populate it using the template."
        )

    key_aliases = {normalize_identifier(x) for x in KEY_COLUMN_ALIASES}
    value_aliases = {normalize_identifier(x) for x in VALUE_COLUMN_ALIASES}

    key_col = next(
        (c for c in df.columns if normalize_identifier(c) in key_aliases),
        df.columns[0],
    )
    value_col = next(
        (c for c in df.columns if normalize_identifier(c) in value_aliases),
        df.columns[1] if len(df.columns) > 1 else df.columns[0],
    )

    config: Dict[str, str] = {}
    for _, row in df.iterrows():
        key_raw = (row.get(key_col) or "").strip()
        if not key_raw:
            continue
        value = row.get(value_col)
        if value is None:
            value = ""
        key_norm = normalize_identifier(key_raw)
        mapped = INSTRUCTION_KEY_ALIASES.get(key_norm)
        if not mapped:
            mapped = key_norm
        config[mapped] = str(value).strip()

    if not config:
        raise ConfigError(
            f"No key/value pairs found in instruction worksheet '{worksheet}'."
        )
    return Config(config, spreadsheet_id, worksheet)


def normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    text = unicodedata.normalize("NFKC", value)
    text = re.sub(r'^[\s"＂\']+|[\s"＂\']+$', "", text)
    return text.strip().lower()


def normalize_company(value: object) -> str:
    base = normalize_text(value)
    base = LEGAL_RE.sub("", base)
    base = re.sub(r"[\s\u3000・･_\-‐-‒–—―()\[\]{}【】『』「」|｜\\/.,，、。]", "", base)
    return base


def clean_domain(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "").strip().lower()
    text = re.sub(r"^(mailto:)?(https?://)?", "", text)
    text = text.lstrip("@")
    text = text.split("/", 1)[0]
    text = text.split("?", 1)[0]
    text = text.split(":", 1)[0]
    return text


def domain_equals_or_sub(candidate: str, ng: str) -> bool:
    return candidate == ng or candidate.endswith("." + ng)


def parse_list_config(raw_value: Optional[str], fallback: Optional[Sequence[str]] = None) -> List[str]:
    if raw_value is not None and str(raw_value).strip():
        parts = re.split(r"[\n,;、]", str(raw_value))
        return [p.strip() for p in parts if p.strip()]
    if fallback is None:
        return []
    return list(fallback)


def ensure_unique_headers(headers: Sequence[str]) -> List[str]:
    seen: Dict[str, int] = {}
    unique: List[str] = []
    for header in headers:
        key = header or "Column"
        count = seen.get(key, 0)
        if count:
            new_header = f"{key}_{count+1}"
        else:
            new_header = key
        seen[key] = count + 1
        unique.append(new_header)
    return unique


def worksheet_to_dataframe(ws: gspread.Worksheet) -> pd.DataFrame:
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header = ensure_unique_headers([h.strip() for h in values[0]])
    rows = values[1:]
    if not rows:
        return pd.DataFrame(columns=header)
    padded = [row + [""] * (len(header) - len(row)) for row in rows]
    df = pd.DataFrame(padded, columns=header)
    df = df.replace({"": pd.NA})
    df = df.dropna(how="all")
    return df


def truthy(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return False
        return value != 0
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y", "on", "有効", "使用", "use"}


def strip_contains_token(raw: str) -> Tuple[str, bool]:
    text = raw.strip()
    if len(text) >= 2 and text[0] in CONTAINS_MARKERS and text[-1] in CONTAINS_MARKERS:
        return text[1:-1].strip(), True
    return text, False


def load_ng_definitions(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_names: Sequence[str],
) -> Tuple[set[str], List[str], set[str], set[str]]:
    if not worksheet_names:
        raise ConfigError("At least one NG tab must be specified in the instruction sheet.")

    ss = client.open_by_key(spreadsheet_id)
    exact_companies: set[str] = set()
    contains_companies: List[str] = []
    ng_emails: set[str] = set()
    ng_domains: set[str] = set()

    normalized_category_aliases = {normalize_identifier(x) for x in CATEGORY_COLUMN_ALIASES}
    normalized_value_aliases = {normalize_identifier(x) for x in NG_VALUE_COLUMN_ALIASES}
    normalized_enabled_aliases = {normalize_identifier(x) for x in ENABLED_COLUMN_ALIASES}
    company_tokens = {normalize_identifier(token) for token in TYPE_COMPANY_TOKENS}
    email_tokens = {normalize_identifier(token) for token in TYPE_EMAIL_TOKENS}

    for worksheet_name in worksheet_names:
        try:
            ws = ss.worksheet(worksheet_name)
        except gspread.WorksheetNotFound as exc:
            raise ConfigError(
                f"NG worksheet '{worksheet_name}' not found in spreadsheet {spreadsheet_id}."
            ) from exc

        df = worksheet_to_dataframe(ws)
        if df.empty:
            continue

        category_col = next(
            (
                c
                for c in df.columns
                if normalize_identifier(c) in normalized_category_aliases
            ),
            df.columns[0],
        )
        value_col = next(
            (
                c
                for c in df.columns
                if normalize_identifier(c) in normalized_value_aliases
            ),
            df.columns[1] if len(df.columns) > 1 else df.columns[0],
        )
        if category_col == value_col and len(df.columns) > 1:
            value_col = df.columns[1]
        enabled_col = next(
            (
                c
                for c in df.columns
                if normalize_identifier(c) in normalized_enabled_aliases
            ),
            None,
        )

        for _, row in df.iterrows():
            if enabled_col and not truthy(row.get(enabled_col)):
                continue
            raw_category = (row.get(category_col) or "").strip()
            raw_value = (row.get(value_col) or "").strip()
            if not raw_category or not raw_value:
                continue

            cat_norm = normalize_identifier(raw_category)
            if cat_norm in company_tokens:
                cleaned, is_contains = strip_contains_token(raw_value)
                norm = normalize_company(cleaned)
                if not norm:
                    continue
                if is_contains:
                    contains_companies.append(norm)
                else:
                    exact_companies.add(norm)
            elif cat_norm in email_tokens:
                cleaned, _ = strip_contains_token(raw_value)
                norm = normalize_text(cleaned)
                if not norm:
                    continue
                if "@" in norm:
                    ng_emails.add(norm)
                else:
                    domain = clean_domain(norm)
                    if domain:
                        ng_domains.add(domain)
            else:
                print(
                    f"[WARN] Unknown NG category '{raw_category}' in tab '{worksheet_name}'. Skipped.",
                    file=sys.stderr,
                )
    return exact_companies, contains_companies, ng_emails, ng_domains


def excel_col_letter(idx0: int) -> str:
    n = idx0 + 1
    label = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        label = chr(65 + remainder) + label
    return label


def build_unsubscribe(base_url: str, mail: object) -> str:
    if not base_url:
        return ""
    if not isinstance(mail, str):
        return ""
    mail = mail.strip()
    if "@" not in mail:
        return ""
    return base_url + mail


def dataframe_to_sheet_values(df: pd.DataFrame) -> List[List[object]]:
    if df.empty:
        return []
    records: List[List[object]] = []
    for row in df.itertuples(index=False, name=None):
        record: List[object] = []
        for value in row:
            if value is None or pd.isna(value):
                record.append("")
            else:
                record.append(value)
        records.append(record)
    return records


def write_dataframe(
    client: gspread.Client,
    spreadsheet_id: str,
    worksheet_name: str,
    df: pd.DataFrame,
) -> None:
    ss = client.open_by_key(spreadsheet_id)
    try:
        ws = ss.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        rows = max(len(df) + 10, 100)
        cols = max(len(df.columns) + 5, 10)
        ws = ss.add_worksheet(title=worksheet_name, rows=str(rows), cols=str(cols))
    ws.clear()
    header = [[col for col in df.columns]]
    values = header + dataframe_to_sheet_values(df)
    if not values:
        values = [[]]
    ws.update("A1", values, value_input_option="USER_ENTERED")


def trim_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()


def is_ng_company(name: object, exact: set[str], contains: Sequence[str]) -> bool:
    normalized = normalize_company(name)
    if not normalized:
        return False
    if normalized in exact:
        return True
    return any(token and token in normalized for token in contains)


def is_ng_email(email: object, ng_emails: set[str], ng_domains: set[str]) -> bool:
    normalized = normalize_text(email)
    if "@" not in normalized:
        return False
    if normalized in ng_emails:
        return True
    domain = normalized.split("@", 1)[1]
    if domain in ng_domains:
        return True
    return any(domain_equals_or_sub(domain, d) for d in ng_domains)


def is_ng_industry(value: object, keywords: Sequence[str]) -> bool:
    if not keywords:
        return False
    normalized = normalize_text(value)
    if not normalized:
        return False
    return any(word and word in normalized for word in keywords)


def find_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    if df.empty:
        return None
    normalized = {col: normalize_identifier(col) for col in df.columns}
    normalized_candidates = [normalize_identifier(c) for c in candidates]
    for candidate in normalized_candidates:
        if not candidate:
            continue
        for col, col_norm in normalized.items():
            if candidate == col_norm or candidate in col_norm:
                return col
    return None


def process(client: gspread.Client, cfg: Config) -> None:
    print("Loading instructions and Google Sheets data...")
    input_spreadsheet = cfg.optional("input_spreadsheet_id", cfg.config_spreadsheet_id)
    input_worksheet = cfg.require("input_worksheet")
    output_spreadsheet = cfg.optional("output_spreadsheet_id", cfg.config_spreadsheet_id)
    output_worksheet = cfg.require("output_worksheet")

    email_candidates = parse_list_config(
        cfg.optional("email_column_candidates"),
        DEFAULT_EMAIL_CANDIDATES,
    )
    company_candidates = parse_list_config(
        cfg.optional("company_column_candidates"),
        DEFAULT_COMPANY_CANDIDATES,
    )
    industry_candidates = parse_list_config(
        cfg.optional("industry_column_candidates"),
        DEFAULT_INDUSTRY_CANDIDATES,
    )
    trim_targets = parse_list_config(
        cfg.optional("trim_columns"),
        DEFAULT_TRIM_COLUMNS,
    )

    industry_keywords = parse_list_config(cfg.optional("industry_keywords"))

    ng_spreadsheet = cfg.optional("ng_spreadsheet_id", cfg.config_spreadsheet_id)
    ng_tabs_raw = cfg.require("ng_tabs")
    ng_tab_names = parse_list_config(ng_tabs_raw)
    if not ng_tab_names:
        raise ConfigError("Instruction sheet NGタブ欄に少なくとも1つ指定してください。")

    input_ws = client.open_by_key(input_spreadsheet).worksheet(input_worksheet)
    df = worksheet_to_dataframe(input_ws)
    if df.empty:
        print("Input worksheet is empty. Nothing to do.")
        write_dataframe(client, output_spreadsheet, output_worksheet, df)
        return

    trim_columns(df, trim_targets)

    email_col = find_column(df, email_candidates)
    company_col = find_column(df, company_candidates)
    industry_col = find_column(df, industry_candidates)

    if not email_col:
        raise ConfigError(
            "Email column could not be detected. Adjust 'メール列候補' in the instruction sheet."
        )
    if not company_col:
        raise ConfigError(
            "Company column could not be detected. Adjust '会社列候補' in the instruction sheet."
        )

    exact_companies, contains_companies, ng_emails, ng_domains = load_ng_definitions(
        client, ng_spreadsheet, ng_tab_names
    )

    unsubscribe_base = cfg.optional("unsubscribe_base_url", "") or ""

    print(
        "Loaded NG definitions:"
        f" exact companies={len(exact_companies)},"
        f" contains companies={len(contains_companies)},"
        f" emails={len(ng_emails)}, domains={len(ng_domains)}"
    )

    mask_company = df[company_col].apply(
        lambda name: is_ng_company(name, exact_companies, contains_companies)
    )
    mask_email = df[email_col].apply(lambda mail: is_ng_email(mail, ng_emails, ng_domains))
    if industry_col:
        mask_industry = df[industry_col].apply(
            lambda value: is_ng_industry(value, industry_keywords)
        )
    else:
        mask_industry = pd.Series(False, index=df.index)

    filtered = df[~mask_company & ~mask_email & ~mask_industry].copy()
    print(
        f"Input rows={len(df)} | ng_company={mask_company.sum()} | ng_email={mask_email.sum()}"
        f" | ng_industry={mask_industry.sum()} | output={len(filtered)}"
    )

    dup_col = "重複チェック"
    if dup_col in filtered.columns:
        filtered.drop(columns=[dup_col], inplace=True)

    email_idx = list(filtered.columns).index(email_col)
    col_letter = excel_col_letter(email_idx)
    start_row = 2
    dup_values: List[str] = []
    if not filtered.empty:
        for offset, _ in enumerate(filtered.itertuples(index=False), start=start_row):
            formula = f"=COUNTIF(${col_letter}${start_row}:${col_letter}${offset},${col_letter}${offset})"
            dup_values.append(formula)
        filtered.insert(email_idx + 1, dup_col, dup_values)
    else:
        filtered.insert(email_idx + 1, dup_col, [])

    unsubscribe_col = "登録解除URL(テキスト)"
    filtered[unsubscribe_col] = filtered[email_col].apply(
        lambda mail: build_unsubscribe(unsubscribe_base, mail)
    )

    write_dataframe(client, output_spreadsheet, output_worksheet, filtered)
    print(f"Done. Wrote {len(filtered)} rows to {output_worksheet}.")

    output_filename = cfg.optional("output_filename")
    if output_filename:
        filtered.to_excel(output_filename, index=False)
        print(f"Saved local Excel file: {output_filename}")


def main() -> None:
    config_spreadsheet = os.environ.get("CONFIG_SPREADSHEET_ID")
    if not config_spreadsheet:
        raise SystemExit("CONFIG_SPREADSHEET_ID environment variable is required")
    config_worksheet = os.environ.get("CONFIG_WORKSHEET", DEFAULT_CONFIG_SHEET)

    client = authorize_from_env()
    cfg = load_config(client, config_spreadsheet, config_worksheet)
    process(client, cfg)


if __name__ == "__main__":
    try:
        main()
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:  # pragma: no cover
        print(f"Unexpected error: {exc}", file=sys.stderr)
        raise
