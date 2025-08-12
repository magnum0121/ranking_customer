"""
環境設定ファイル
Windows/WSL環境に対応した動的パス設定
"""

import os
import datetime
from pathlib import Path


def get_base_path() -> Path:
    """
    実行環境に応じてベースパスを返す
    
    Returns:
        Path: 環境に応じたベースパス
    """
    if os.name == 'nt':  # Windows環境
        return Path("D:/Skit_Actual")
    else:  # WSL/Linux環境
        return Path("/mnt/d/Skit_Actual")


# ベースパス設定
BASE_PATH = get_base_path()

# 各ディレクトリパス設定
CSV_DIR = BASE_PATH / "CSV"
EXCEL_DIR = BASE_PATH / "EXCEL"
MASTER_DIR = BASE_PATH / "PG" / "累計" / "マスタ"

# よく使用されるファイルパス
CSV_FILE_PATH = CSV_DIR / "実績.csv"
MASTER_FILE_PATH = MASTER_DIR / "法人マスタ.csv"

# 共通設定
TERM = "年度"
START_YEAR = 2015
SHEET_NAMES = ("法人", "得意先", "得意先（商品）")

def get_fiscal_years(base_year: int = 2025) -> tuple[str, str, int]:
    """
    現在の月に基づいて年度を計算する
    
    Args:
        base_year: 基準年（デフォルト: 2025）
    
    Returns:
        tuple[str, str, int]: (当年度, 前年度, 現在年度)
    """
    current_year = base_year
    
    if datetime.datetime.now().month >= 4:
        year = str(current_year)
        last_year = str(current_year - 1)
    else:
        current_year -= 1
        year = str(current_year)
        last_year = str(current_year - 1)
    
    return year, last_year, current_year

def get_excel_output_path(term: str) -> Path:
    """
    Excel出力パスを取得
    
    Args:
        term: 期間（"年度"など）
    
    Returns:
        Path: Excel出力パス
    """
    return EXCEL_DIR / "累計" / f"{term}_ランキング（得意先）.xlsx"


def get_past_excel_path(last_year: str, term: str) -> Path:
    """
    過去実績Excelファイルパスを取得
    
    Args:
        last_year: 前年度
        term: 期間（"年度"など）
    
    Returns:
        Path: 過去実績Excelファイルパス
    """
    return EXCEL_DIR / last_year / "累計" / f"{term}_ランキング（得意先）.xlsx"


def get_past_parquet_dir() -> Path:
    """
    昨年度データのparquetディレクトリパスを取得
    
    Returns:
        Path: 昨年度parquetディレクトリパス
    """
    return BASE_PATH / "PG" / "累計" / "昨年度データ" / "parquet"


def get_past_parquet_path(last_year: str, term: str, data_type: str) -> Path:
    """
    過去実績Parquetファイルパスを取得
    
    Args:
        last_year: 前年度
        term: 期間（"年度"など）
        data_type: データ種別（"法人", "得意先", "得意先（商品）"）
    
    Returns:
        Path: 過去実績Parquetファイルパス
    """
    parquet_dir = get_past_parquet_dir()
    filename = f"{term}_ランキング（得意先）_{data_type}.parquet"
    return parquet_dir / filename


def ensure_directories():
    """
    必要なディレクトリが存在しない場合は作成する
    """
    directories = [CSV_DIR, EXCEL_DIR, MASTER_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)


# 環境情報の表示（デバッグ用）
def print_environment_info():
    """
    現在の環境情報を表示（デバッグ用）
    """
    print(f"OS: {os.name}")
    print(f"ベースパス: {BASE_PATH}")
    print(f"CSV dir: {CSV_DIR}")
    print(f"Excel dir: {EXCEL_DIR}")
    print(f"Master dir: {MASTER_DIR}")


if __name__ == "__main__":
    print_environment_info()