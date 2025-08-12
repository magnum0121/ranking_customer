"""
年度ランキング（得意先）データ作成スクリプト
実績CSVから各階層別にデータを集計し、parquetファイルとして出力する
"""

import datetime
import time
import polars as pl
import pretty_errors
from pathlib import Path
from rich import print
from rich.logging import RichHandler
import logging
from config import CSV_FILE_PATH, TERM, get_fiscal_years

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)




def load_and_process_data(file_path: str, usecols: list, year: str) -> pl.DataFrame:
    """
    CSVファイルを読み込み、列名をリネームして集計する
    
    Args:
        file_path: CSVファイルのパス
        usecols: 使用する列のリスト
        year: 年度文字列
    
    Returns:
        pl.DataFrame: 処理済みのデータフレーム
    """
    # データ型定義
    schema = {
        "法人コード": pl.Utf8,
        "売上数": pl.Float64,  # 売上数はfloat型
        "㎡": pl.Float64,
        "売上金額": pl.Float64,  # 売上金額はfloatで読み込み
        "粗利金額": pl.Float64,  # 粗利金額はfloatで読み込み
        "得意先コード": pl.Utf8,
        "得意先名": pl.Utf8,
        "営業所名": pl.Utf8,
        # "担当者名": pl.Utf8,  # 担当者名は使用しない
        "第1階層": pl.Utf8,
        "商品コード": pl.Utf8,
        "商品名": pl.Utf8
    }
    
    # CSVファイル読み込み
    df = pl.read_csv(file_path, columns=usecols, encoding="cp932", schema_overrides=schema)
    
    # 列名をリネーム（年度を付与）
    rename_columns = {
        "売上数": f"{year}売上数",
        "㎡": f"{year}㎡",
        "売上金額": f"{year}売上",
        "粗利金額": f"{year}粗利"
    }
    
    return df.select([
        pl.col(col).alias(rename_columns.get(col, col)) for col in usecols
    ])


def create_output_filename(data_type: str) -> str:
    """
    出力ファイル名を作成
    
    Args:
        data_type: データタイプ（"法人", "得意先", "得意先商品"）
    
    Returns:
        str: 出力ファイル名
    """
    return f"{TERM}_ランキング（得意先）_{data_type}.parquet"

def process_data(df: pl.DataFrame, group_cols: list, year: str) -> pl.DataFrame:
    """
    データを処理して集計
    
    Args:
        df: 入力データフレーム
        group_cols: グループ化キー
        year: 年度
    
    Returns:
        pl.DataFrame: 集計済みデータフレーム
    """
    # 集計処理
    agg_exprs = [
        pl.col(f"{year}売上数").sum(),
        pl.col(f"{year}㎡").sum(),
        pl.col(f"{year}売上").sum().cast(pl.Int64),
        pl.col(f"{year}粗利").sum().cast(pl.Int64)
    ]
    
    return df.group_by(group_cols).agg(agg_exprs)

def main():
    """メイン処理"""
    start_time = time.time()
    
    # ===== 設定 =====
    # 必要列定義
    COLUMNS_CONFIG = {
        "法人": ["法人コード", "売上数", "㎡", "売上金額", "粗利金額"],
        "得意先": ["得意先コード", "得意先名", "売上数", "㎡", "売上金額", "粗利金額"],
        "得意先商品": [
            "営業所名", # "担当者名",  # 担当者名は使用しない
            "得意先コード", "得意先名", "第1階層",
            "商品コード", "商品名", "売上数", "㎡", "売上金額", "粗利金額"
        ]
    }
    
    # ===== 年度計算 =====
    year, last_year, current_year = get_fiscal_years()
    logger.info(f"処理対象年度: {year} (前年度: {last_year})")
    
    # ===== ファイル存在確認 =====
    if not Path(CSV_FILE_PATH).exists():
        logger.error(f"ファイルが見つかりません - {CSV_FILE_PATH}")
        return
    
    try:
        # ===== データ読み込み・処理 =====
        logger.info("データ読み込み開始...")
        
        # 一度に必要なすべての列を読み込む
        all_columns = set()
        for cols in COLUMNS_CONFIG.values():
            all_columns.update(cols)
        
        # CSVファイルを一度だけ読み込む
        df = pl.read_csv(
            CSV_FILE_PATH,
            encoding="cp932",
            schema_overrides={
                "法人コード": pl.Utf8,
                "売上数": pl.Float64,
                "㎡": pl.Float64,
                "売上金額": pl.Float64,
                "粗利金額": pl.Float64,
                "得意先コード": pl.Utf8,
                "得意先名": pl.Utf8,
                "営業所名": pl.Utf8,
                # "担当者名": pl.Utf8,  # 担当者名は使用しない
                "第1階層": pl.Utf8,
                "商品コード": pl.Utf8,
                "商品名": pl.Utf8
            }
        )
        
        # 必要な列のみを選択
        df = df.select([col for col in all_columns if col in df.columns])
        
        # 列名をリネーム（年度を付与）
        rename_columns = {
            "売上数": f"{year}売上数",
            "㎡": f"{year}㎡",
            "売上金額": f"{year}売上",
            "粗利金額": f"{year}粗利"
        }
        
        # リネーム対象の列のみを選択
        selected_columns = []
        for col in df.columns:
            if col in rename_columns:
                selected_columns.append(pl.col(col).alias(rename_columns[col]))
            elif col in all_columns:  # 元の列名がall_columnsに含まれている場合
                selected_columns.append(pl.col(col))
        
        df = df.select(selected_columns)
        
        # 法人別データ
        corporation_columns = []
        for col in COLUMNS_CONFIG["法人"]:
            if col in rename_columns:
                corporation_columns.append(pl.col(rename_columns[col]))
            elif col in df.columns:
                corporation_columns.append(pl.col(col))
        df_corporation = df.select(corporation_columns)
        df_corporation_grouped = process_data(df_corporation, ["法人コード"], year)
        
        # 得意先別データ
        customer_columns = []
        for col in COLUMNS_CONFIG["得意先"]:
            if col in rename_columns:
                customer_columns.append(pl.col(rename_columns[col]))
            elif col in df.columns:
                customer_columns.append(pl.col(col))
        df_customer = df.select(customer_columns)
        df_customer_grouped = process_data(df_customer, ["得意先コード", "得意先名"], year)
        
        # 得意先商品別データ
        customer_product_columns = []
        for col in COLUMNS_CONFIG["得意先商品"]:
            if col in rename_columns:
                customer_product_columns.append(pl.col(rename_columns[col]))
            elif col in df.columns:
                customer_product_columns.append(pl.col(col))
        df_customer_product = df.select(customer_product_columns)
        df_customer_product_grouped = process_data(
            df_customer_product,
            ["営業所名", # "担当者名",  # 担当者名は使用しない
            "得意先コード", "得意先名", "第1階層", "商品コード", "商品名"],
            year
        )
        
        # ===== ファイル出力 =====
        logger.info("ファイル出力開始...")
        
        output_files = [
            (df_corporation_grouped, create_output_filename("法人")),
            (df_customer_grouped, create_output_filename("得意先")),
            (df_customer_product_grouped, create_output_filename("得意先（商品）"))
        ]
        
        for df, filename in output_files:
            df.write_parquet(filename)
            logger.info(f"出力完了: {filename}")
        
        # ===== 完了メッセージ =====
        logger.info("[green]当年実績_作成完了[/green]")
        
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
        raise
    
    finally:
        # ===== 処理時間表示 =====
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info(f"処理時間: {elapsed_time:.2f}秒")


if __name__ == "__main__":
    main()
