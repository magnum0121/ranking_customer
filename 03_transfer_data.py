"""
データ転記スクリプト（整理版）
parquetファイルからExcelファイルにデータを転記する

機能:
- 転記用データディレクトリからparquetファイルを読み込み
- Excelフォーマットファイルを開いて各シートにデータ転記
- 指定された出力先に結果を保存

依存関係:
- polars: 高速データ読み込み
- pandas: Excel互換性
- xlwings: Excel操作
- rich: ログ出力とエラー表示
"""

import time
import gc
from typing import Dict, Optional, Tuple
from pathlib import Path

import polars as pl
import pandas as pd
import xlwings as xw
import pretty_errors
from rich import print
from rich.logging import RichHandler
import logging

from config import get_excel_output_path, TERM, SHEET_NAMES

# ===== 設定セクション =====
class TransferConfig:
    """転記処理の設定を管理するクラス"""
    
    # ディレクトリとファイル設定
    OUTPUT_DIR: str = "転記用データ"
    SOURCE_FILE: str = "ランキング（得意先）_フォーマット.xlsx"
    
    # 処理設定
    CHUNK_SIZE: int = 1000  # Excel転記時のチャンクサイズ
    ENCODING: str = 'cp932'  # Excel文字エンコーディング
    
    # ファイルマッピング
    @classmethod
    def get_file_mapping(cls) -> Dict[str, str]:
        """各シートに対応するparquetファイルのマッピングを返す"""
        return {
            "法人": f"{cls.OUTPUT_DIR}/法人_転記用データ.parquet",
            "得意先": f"{cls.OUTPUT_DIR}/得意先_転記用データ.parquet", 
            "得意先（商品）": f"{cls.OUTPUT_DIR}/得意先（商品）_転記用データ.parquet"
        }

# ===== ロギング設定 =====
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)


# ===== コア処理関数 =====
class ExcelTransferManager:
    """Excel転記処理を管理するクラス"""
    
    def __init__(self):
        self.app: Optional[xw.App] = None
        self.workbook: Optional[xw.Book] = None
        
    def __enter__(self):
        """コンテキストマネージャー開始"""
        self.app = self._setup_excel()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """コンテキストマネージャー終了（リソース解放）"""
        self._cleanup_resources()
        
    def _setup_excel(self) -> xw.App:
        """Excel設定を初期化
        
        Returns:
            xw.App: 初期化されたExcelアプリケーション
            
        Raises:
            Exception: Excel初期化に失敗した場合
        """
        try:
            app = xw.App(visible=False)
            logger.info("-> Excel アプリケーション初期化完了")
            return app
        except Exception as e:
            logger.error(f"❌ Excel初期化エラー: {e}")
            raise
    
    def _cleanup_resources(self) -> None:
        """リソースの安全なクリーンアップ"""
        try:
            if self.workbook:
                self.workbook.close()
                logger.debug("-> ワークブック クローズ完了")
                
            if self.app:
                self.app.quit()
                logger.debug("-> Excel アプリケーション 終了完了")
                
            # ガベージコレクション実行
            gc.collect()
            
        except Exception as e:
            logger.warning(f"⚠️ リソースクリーンアップ中にエラー: {e}")
    
    def open_workbook(self, source_file: str) -> None:
        """ワークブックを開く
        
        Args:
            source_file: 開くファイルのパス
            
        Raises:
            FileNotFoundError: ファイルが見つからない場合
            Exception: ワークブック開くのに失敗した場合
        """
        try:
            if not Path(source_file).exists():
                raise FileNotFoundError(f"ソースファイルが見つかりません: {source_file}")
                
            self.workbook = self.app.books.open(source_file)
            logger.info(f"-> ワークブック読み込み完了: {source_file}")
            
        except Exception as e:
            logger.error(f"❌ ワークブック読み込みエラー: {source_file}")
            raise
    
    def save_workbook(self, save_path: str) -> None:
        """ワークブックを保存
        
        Args:
            save_path: 保存先パス
            
        Raises:
            Exception: 保存に失敗した場合
        """
        try:
            if not self.workbook:
                raise ValueError("保存するワークブックがありません")
                
            # 保存先ディレクトリが存在しない場合は作成
            Path(save_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.workbook.save(save_path)
            logger.info(f"-> 保存完了: {save_path}")
            
        except Exception as e:
            logger.error(f"❌ 保存エラー: {save_path} - {e}")
            raise


def load_parquet_data(parquet_path: str) -> pl.DataFrame:
    """parquetファイルからDataFrameを読み込み
    
    Args:
        parquet_path: 読み込むparquetファイルのパス
        
    Returns:
        pl.DataFrame: 読み込んだデータフレーム
        
    Raises:
        FileNotFoundError: ファイルが見つからない場合
        Exception: データ読み込みに失敗した場合
    """
    try:
        if not Path(parquet_path).exists():
            raise FileNotFoundError(f"Parquetファイルが見つかりません: {parquet_path}")
        
        # polarsで高速読み込み
        df = pl.read_parquet(parquet_path)
        
        # データの基本情報をログ出力
        row_count, col_count = df.shape
        file_name = Path(parquet_path).name
        logger.info(f"-> データ読み込み完了: {file_name} ({row_count:,}行, {col_count}列)")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ データ読み込みエラー: {parquet_path} - {e}")
        raise


def get_optimal_chunk_size(row_count: int) -> int:
    """データ量に応じた最適なチャンクサイズを計算
    
    Args:
        row_count: データの行数
        
    Returns:
        int: 最適なチャンクサイズ
    """
    if row_count > 50000:
        return 2000
    elif row_count > 10000: 
        return 1000
    else:
        return 500


def transfer_data_to_sheet(
    excel_manager: ExcelTransferManager, 
    sheet_name: str, 
    dataframe: pl.DataFrame
) -> None:
    """DataFrameをExcelシートに転記
    
    Args:
        excel_manager: Excel転記管理オブジェクト
        sheet_name: 転記先シート名
        dataframe: 転記するデータフレーム
        
    Raises:
        KeyError: シートが見つからない場合
        Exception: 転記処理に失敗した場合
    """
    try:
        if not excel_manager.workbook:
            raise ValueError("ワークブックが開かれていません")
            
        # シートの存在確認
        if sheet_name not in [sheet.name for sheet in excel_manager.workbook.sheets]:
            raise KeyError(f"シート '{sheet_name}' が見つかりません")
        
        target_sheet = excel_manager.workbook.sheets[sheet_name]
        
        # null値を0で埋める（Excel互換性のため）
        df_cleaned = dataframe.fill_null(0)
        
        # 最適なチャンクサイズを決定
        row_count = len(df_cleaned)
        chunk_size = get_optimal_chunk_size(row_count)
        
        # pandas形式に変換（xlwings互換性のため）
        pandas_df = df_cleaned.to_pandas()
        
        # 効率的な転記実行
        target_sheet.range('A1').options(
            index=False,
            header=True,
            encoding=TransferConfig.ENCODING,
            strings_to_numbers=True,
            chunksize=chunk_size
        ).value = pandas_df
        
        logger.info(f"-> {sheet_name}シート転記完了 ({row_count:,}行, チャンク: {chunk_size})")
        
        # メモリクリーンアップ
        del pandas_df, df_cleaned
        gc.collect()
        
    except Exception as e:
        logger.error(f"❌ {sheet_name}シート転記エラー: {e}")
        raise


def validate_prerequisites() -> Tuple[bool, list]:
    """転記処理の前提条件をチェック
    
    Returns:
        Tuple[bool, list]: (チェック結果, 不足ファイルリスト)
    """
    missing_files = []
    
    # ソースファイルの存在確認
    if not Path(TransferConfig.SOURCE_FILE).exists():
        missing_files.append(TransferConfig.SOURCE_FILE)
    
    # parquetファイルの存在確認
    file_mapping = TransferConfig.get_file_mapping()
    for sheet_name, parquet_path in file_mapping.items():
        if not Path(parquet_path).exists():
            missing_files.append(f"{parquet_path} (シート: {sheet_name})")
    
    return len(missing_files) == 0, missing_files


def execute_transfer_process() -> bool:
    """データ転記処理を実行
    
    Returns:
        bool: 処理成功フラグ
    """
    start_time = time.time()
    
    try:
        logger.info("📊 データ転記処理を開始します")
        
        # 前提条件チェック
        is_valid, missing_files = validate_prerequisites()
        if not is_valid:
            logger.error("❌ 必要ファイルが不足しています:")
            for file in missing_files:
                logger.error(f"   - {file}")
            return False
        
        # 設定情報を取得
        file_mapping = TransferConfig.get_file_mapping()
        save_path = str(get_excel_output_path(TERM))
        
        # Excel転記処理をコンテキストマネージャーで実行
        with ExcelTransferManager() as excel_manager:
            # ワークブック読み込み
            excel_manager.open_workbook(TransferConfig.SOURCE_FILE)
            
            # 各シートにデータ転記
            for sheet_name in SHEET_NAMES:
                parquet_path = file_mapping[sheet_name]
                
                # データ読み込み
                df = load_parquet_data(parquet_path)
                
                # シートに転記
                transfer_data_to_sheet(excel_manager, sheet_name, df)
                
                # メモリクリーンアップ
                del df
                gc.collect()
            
            # ワークブック保存
            excel_manager.save_workbook(save_path)
        
        # 処理完了ログ
        end_time = time.time()
        elapsed_time = end_time - start_time
        
        logger.info("✅ 全ての処理が正常に完了しました")
        logger.info(f"⏱️ 処理時間: {elapsed_time:.2f}秒")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 処理中にエラーが発生しました: {str(e)}")
        return False


def main() -> None:
    """メイン処理
    
    転記処理を実行し、結果に応じて適切な終了コードを設定
    """
    try:
        success = execute_transfer_process()
        
        if not success:
            print("❌ [bold red]データ転記処理が失敗しました[/bold red]")
            exit(1)
        else:
            print("✅ [bold green]データ転記処理が完了しました[/bold green]")
            exit(0)
            
    except KeyboardInterrupt:
        logger.warning("⚠️ ユーザーによって処理が中断されました")
        exit(2)
    except Exception as e:
        logger.error(f"❌ 予期しないエラーが発生しました: {e}")
        exit(1)


if __name__ == "__main__":
    main()
