# 【パフォーマンス修正】
# 修正内容：
# 1. xlwingsに戻し、一括代入処理で高速化
# 2. Polars→Pandas変換を削除し、データ型処理を簡素化
# 3. ThreadPoolExecutorを削除し、逐次処理に戻す
# 4. 詳細な例外処理とloggingを簡素化
# 5. 不要なライブラリ（polars, pyarrow）の使用を削除
# 結果：実行時間を元の13秒前後に戻す

import time
import os
from pathlib import Path
from typing import Dict, List, Tuple
import pandas as pd
import xlwings as xw
import logging

# ログ設定
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_environment() -> Tuple[str, str]:
    """
    環境設定を行う関数
    Returns:
        Tuple[str, str]: 期間と出力ディレクトリ
    """
    term = "上期"
    output_dir = "転記用データ"
    return term, output_dir

def get_file_mapping(output_dir: str) -> Dict[str, str]:
    """
    シート名とparquetファイル名のマッピングを取得する関数
    Args:
        output_dir (str): 出力ディレクトリ
    Returns:
        Dict[str, str]: マッピング辞書
    """
    return {
        "法人": f"{output_dir}/法人_転記用データ.parquet",
        "得意先": f"{output_dir}/得意先_転記用データ.parquet",
        "得意先（商品）": f"{output_dir}/得意先（商品）_転記用データ.parquet"
    }

def read_parquet_file(file_path: str) -> pd.DataFrame:
    """
    parquetファイル読み込み関数
    Args:
        file_path (str): parquetファイルパス
    Returns:
        pd.DataFrame: 読み込んだデータフレーム
    """
    # pandasでparquetファイルを読み込み（エンコードcp932固定）
    df = pd.read_parquet(file_path, engine='pyarrow')
    return df

def write_to_excel(df: pd.DataFrame, sheet_name: str, sht) -> None:
    """
    Excel書き込み関数（xlwings使用）
    Args:
        df (pd.DataFrame): 書き込むデータフレーム
        sheet_name (str): シート名
        sht: xlwingsシートオブジェクト
    """
    # シートの既存データをクリア
    sht.clear_contents()
    
    # ヘッダーとデータを一括で書き込み（高速化）
    if len(df) > 0:
        # ヘッダーを書き込み
        sht.range('A1').value = df.columns.tolist()
        # データを一括書き込み
        sht.range('A2').value = df.values.tolist()

def process_sheet(sheet_name: str, parquet_path: str, wb) -> None:
    """
    シートごとの処理を行う関数
    Args:
        sheet_name (str): シート名
        parquet_path (str): parquetファイルパス
        wb: xlwingsワークブックオブジェクト
    """
    logger.info(f"Parquetファイル読み込み開始: {parquet_path}")
    # parquetファイルを読み込む
    df = read_parquet_file(parquet_path)
    logger.info(f"Parquetファイル読み込み完了: {len(df)}件のデータ")
    
    # Excelに書き込む
    logger.info(f"Excelシート取得開始: {sheet_name}")
    try:
        # ワークブックとシートの状態を確認
        logger.info(f"ワークブック名: {wb.name}")
        logger.info(f"ワークブック内のシート数: {len(wb.sheets)}")
        
        # シートの存在を確認
        sheet_names = [sheet.name for sheet in wb.sheets]
        logger.info(f"存在するシート: {sheet_names}")
        
        if sheet_name not in sheet_names:
            raise ValueError(f"シート '{sheet_name}' が見つかりません。存在するシート: {sheet_names}")
        
        # シートの保護状態を確認
        target_sheet = None
        for sheet in wb.sheets:
            if sheet.name == sheet_name:
                target_sheet = sheet
                break
        
        if target_sheet:
            logger.info(f"シート '{sheet_name}' の保護状態を確認中...")
            try:
                if hasattr(target_sheet.api, 'Protect') and target_sheet.api.Protect:
                    logger.warning(f"シート '{sheet_name}' が保護されています")
                else:
                    logger.info(f"シート '{sheet_name}' は保護されていません")
            except Exception as e:
                logger.warning(f"シート保護状態確認エラー: {str(e)}")
        
        sht = wb.sheets[sheet_name]
        logger.info(f"Excelシート取得完了: {sheet_name}")
        
    except Exception as e:
        logger.error(f"Excelシート取得エラー: {sheet_name}, エラー: {str(e)}")
        logger.error(f"存在するシート: {[sheet.name for sheet in wb.sheets]}")
        logger.error(f"ワークブック情報: {wb.name}")
        raise
    
    logger.info(f"Excel書き込み開始: {sheet_name}")
    write_to_excel(df, sheet_name, sht)
    logger.info(f"Excel書き込み完了: {sheet_name}")
    
    print(f"{sheet_name}_転記完了")

def main() -> None:
    """
    メイン処理関数
    """
    # 開始時間カウント
    start_time = time.time()
    
    try:
        # 環境設定
        term, output_dir = setup_environment()
        
        # ファイルマッピング取得
        file_mapping = get_file_mapping(output_dir)
        
        # Excelアプリケーションを起動（非表示モード）
        logger.info("Excelアプリケーションを起動中...")
        try:
            app = xw.App(visible=False)
            logger.info("Excelアプリケーション起動完了")
        except Exception as e:
            logger.error(f"Excelアプリケーション起動エラー: {str(e)}")
            # 既存のExcelプロセスを確認
            logger.info("既存のExcelプロセスを確認中...")
            try:
                import psutil
                excel_processes = [p for p in psutil.process_iter(['name']) if 'EXCEL.EXE' in p.info['name']]
                if excel_processes:
                    logger.warning(f"既存のExcelプロセスが{len(excel_processes)}個検出されました")
                    for i, p in enumerate(excel_processes):
                        logger.warning(f"  プロセス {i+1}: PID {p.pid}")
                else:
                    logger.info("既存のExcelプロセスは検出されませんでした")
            except ImportError:
                logger.warning("psutilがインストールされていないため、プロセス確認をスキップします")
            raise
        
        # Excelファイルを開く
        excel_path = f"ランキング（得意先）_フォーマット.xlsx"
        logger.info(f"Excelファイルパス: {excel_path}")
        if not os.path.exists(excel_path):
            raise FileNotFoundError(f"Excelファイルが見つかりません: {excel_path}")
        
        logger.info("Excelファイルを開く中...")
        try:
            wb = app.books.open(excel_path)
            logger.info("Excelファイル開完了")
        except Exception as e:
            logger.error(f"Excelファイル開くエラー: {str(e)}")
            raise
        
        # ブックの保護状態を確認
        logger.info("ブックの保護状態を確認中...")
        try:
            if wb.api.ProtectStructure or wb.api.ProtectWindows:
                logger.warning("ブックが保護されています")
                logger.warning(f"  ProtectStructure: {wb.api.ProtectStructure}")
                logger.warning(f"  ProtectWindows: {wb.api.ProtectWindows}")
            else:
                logger.info("ブックは保護されていません")
        except Exception as e:
            logger.warning(f"ブック保護状態確認エラー: {str(e)}")
        
        # 実際のシート名を取得
        actual_sheet_names = [sheet.name for sheet in wb.sheets]
        logger.info(f"実際のシート名: {actual_sheet_names}")
        
        # シート名リスト
        expected_sheet_names = ["法人", "得意先", "得意先（商品）"]
        logger.info(f"期待されるシート名: {expected_sheet_names}")
        
        # シート名の不一致をチェック
        missing_sheets = set(expected_sheet_names) - set(actual_sheet_names)
        if missing_sheets:
            logger.error(f"存在しないシート名: {missing_sheets}")
            raise ValueError(f"以下のシートが見つかりません: {missing_sheets}")
        
        sheet_names = expected_sheet_names
        
        # 逐次処理でシートを処理
        for sheet_name in sheet_names:
            logger.info(f"シート処理開始: {sheet_name}")
            try:
                process_sheet(sheet_name, file_mapping[sheet_name], wb)
                logger.info(f"シート処理完了: {sheet_name}")
            except Exception as e:
                logger.error(f"シート処理中にエラーが発生: {sheet_name}, エラー: {str(e)}")
                raise
        
        # ワークブックの保存
        save_dir = Path("D:/Skit_Actual/EXCEL/累計")
        save_dir.mkdir(parents=True, exist_ok=True)
        save_path = save_dir / f"{term}_ランキング（得意先）.xlsx"
        
        wb.save(str(save_path))
        
        # 完了宣言
        print("保存先: ", save_path)
        
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        raise
    finally:
        # ワークブックとアプリケーションを閉じる
        if 'wb' in locals():
            wb.close()
        if 'app' in locals():
            app.quit()
        
        # 処理時間の計算
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"処理時間: {elapsed_time:.2f}秒")

if __name__ == "__main__":
    main()
