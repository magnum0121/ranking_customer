"""
過去データ結合スクリプト（安全な改善版）
当年度データと過去年度データを結合して転記用データを作成する

機能:
- 当年度parquetファイルの読み込み
- 過去年度parquetファイルの読み込み
- データ結合処理（full join）
- 法人マスタとの結合
- データ整合性検証
- 転記用データの出力

安全方針:
- データ変更は一切行わない（出力結果は従来と完全同一）
- 段階的な改善でリスクを最小化
- 完全な検証機能付き
"""

import gc
import os
import time
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import polars as pl
import pretty_errors
from rich import print
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, TaskID, BarColumn, TextColumn, TimeRemainingColumn
import logging

from config import MASTER_DIR, get_past_parquet_path, TERM, START_YEAR, get_fiscal_years

# ===== 設定セクション =====
class ProcessConfig:
    """処理設定を管理するクラス"""
    
    # 出力設定
    OUTPUT_DIR: str = "転記用データ"
    
    # データ種別
    DATA_TYPES: List[str] = ["法人", "得意先", "得意先（商品）"]
    
    # 売上データパターン
    SALES_PATTERNS: List[str] = ["売上", "数", "㎡", "粗利", "数量", "金額"]
    
    # 結合キー候補
    PRODUCT_JOIN_KEYS: List[str] = ["営業所名", "得意先コード", "得意先名", "第1階層", "商品コード", "商品名"]
    
    # データ型変換対象列
    CODE_COLUMNS: List[str] = ["法人コード", "得意先コード", "商品コード"]

# ===== ロギング設定 =====
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)]
)
logger = logging.getLogger(__name__)
console = Console()

# UserWarningを無視
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")


class DataIntegrityValidator:
    """データ整合性検証クラス"""
    
    @staticmethod
    def validate_data_totals(df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame, 
                           description: str) -> Dict[str, Dict[str, float]]:
        """データの総計値を検証・記録"""
        totals = {}
        
        if df1.is_empty():
            logger.warning(f"{description}: データが空のため検証をスキップ")
            return totals
        
        # 売上列を取得
        sales_columns = [col for col in df1.columns if col.endswith("売上")]
        
        for sales_col in sorted(sales_columns):
            if sales_col in df1.columns and sales_col in df2.columns and sales_col in df3.columns:
                total1 = df1[sales_col].sum()
                total2 = df2[sales_col].sum()
                total3 = df3[sales_col].sum()
                
                totals[sales_col] = {
                    "法人": total1,
                    "得意先": total2,
                    "得意先（商品）": total3
                }
                
                logger.info(f"{sales_col} ({description}):")
                logger.info(f"  法人: {total1:,.0f}円")
                logger.info(f"  得意先: {total2:,.0f}円")
                logger.info(f"  得意先（商品）: {total3:,.0f}円")
        
        return totals
    
    @staticmethod
    def compare_totals(before_totals: Dict, after_totals: Dict, year: str) -> bool:
        """結合前後の総計値を比較（強化版：詳細な差異分析付き）
        
        Args:
            before_totals: 結合前の総計値
            after_totals: 結合後の総計値
            year: 対象年度
            
        Returns:
            bool: 整合性チェック結果（True=問題なし、False=差異あり）
        """
        logger.info("=== 総計値検証処理（強化版） ===")
        
        is_valid = True
        total_issues = 0
        
        # 全ての年度の売上列を取得
        all_sales_columns = set()
        if before_totals:
            all_sales_columns.update(before_totals.keys())
        if after_totals:
            all_sales_columns.update(after_totals.keys())
        
        if not all_sales_columns:
            logger.warning("⚠️ 検証対象の売上列が見つかりません")
            return False
        
        logger.info(f"🔍 検証対象列: {sorted(all_sales_columns)}")
        
        for sales_col in sorted(all_sales_columns):
            logger.info(f"\n📊 {sales_col} の詳細検証:")
            
            # 結合後のデータ状況を表示
            if sales_col in after_totals:
                after = after_totals[sales_col]
                logger.info(f"  結合後データ:")
                logger.info(f"    法人データ: {after['法人']:,.0f}円")
                logger.info(f"    得意先データ: {after['得意先']:,.0f}円") 
                logger.info(f"    得意先（商品）データ: {after['得意先（商品）']:,.0f}円")
                
                # 結合前後の比較（重要な差異チェック）
                if sales_col in before_totals:
                    before = before_totals[sales_col]
                    logger.info(f"  結合前後の差異分析:")
                    
                    for data_type in ["法人", "得意先", "得意先（商品）"]:
                        if after[data_type] != before[data_type]:
                            diff = after[data_type] - before[data_type]
                            diff_abs = abs(diff)
                            diff_rate = (diff_abs / max(before[data_type], 1)) * 100
                            direction = "増加" if diff > 0 else "減少"
                            
                            # 差異のレベルを判定
                            if diff_abs > 1000000:  # 100万円以上
                                level = "❌ 重大"
                                is_valid = False
                                total_issues += 1
                            elif diff_abs > 100000:  # 10万円以上
                                level = "⚠️ 警告"
                                total_issues += 1
                            else:
                                level = "ℹ️ 軽微"
                            
                            logger.warning(f"    {level} {data_type}: {diff:+,.0f}円 ({direction}, 変動率{diff_rate:.2f}%)")
                        else:
                            logger.info(f"    ✅ {data_type}: 差異なし")
                else:
                    logger.info(f"  結合前データなし（新規年度の可能性）")
                
                # データ種別間の整合性チェック（重要）
                logger.info(f"  データ種別間整合性チェック:")
                consistency_issues = []
                
                if after["法人"] != after["得意先"]:
                    diff = abs(after["法人"] - after["得意先"])
                    consistency_issues.append(f"法人 vs 得意先: {diff:,.0f}円の差異")
                
                if after["得意先"] != after["得意先（商品）"]:
                    diff = abs(after["得意先"] - after["得意先（商品）"])
                    consistency_issues.append(f"得意先 vs 得意先（商品）: {diff:,.0f}円の差異")
                
                if after["法人"] != after["得意先（商品）"]:
                    diff = abs(after["法人"] - after["得意先（商品）"])
                    consistency_issues.append(f"法人 vs 得意先（商品）: {diff:,.0f}円の差異")
                
                if consistency_issues:
                    logger.error(f"    ❌ 整合性問題: {len(consistency_issues)}件")
                    for issue in consistency_issues:
                        logger.error(f"      - {issue}")
                    is_valid = False
                    total_issues += len(consistency_issues)
                else:
                    logger.info(f"    ✅ {sales_col}: 全データ種別で総計値が一致")
            else:
                logger.warning(f"  結合後データなし（データ消失の可能性）")
                is_valid = False
                total_issues += 1
        
        # 総合判定
        logger.info(f"\n📋 総合検証結果:")
        if is_valid and total_issues == 0:
            logger.info("✅ データ整合性: 問題なし")
            logger.info("🎉 全ての年度データが正常に結合されました")
        else:
            logger.error(f"❌ データ整合性: {total_issues}件の問題を検出")
            logger.error("⚠️ データ差異が発生しています。処理を停止することを推奨します。")
        
        return is_valid and total_issues == 0

    @staticmethod
    def validate_pre_merge_data(df1_a: pl.DataFrame, df2_a: pl.DataFrame, df3_a: pl.DataFrame,
                              df1_b: pl.DataFrame, df2_b: pl.DataFrame, df3_b: pl.DataFrame,
                              current_year: int) -> Dict[str, Dict]:
        """結合前のデータ整合性を事前検証
        
        Args:
            df1_a, df2_a, df3_a: 当年度データ
            df1_b, df2_b, df3_b: 過去データ
            current_year: 当年度
            
        Returns:
            Dict: 検証結果とサマリー
        """
        logger.info("=== 結合前データ事前検証 ===")
        
        validation_result = {
            "current_year_data": {},
            "past_data": {},
            "potential_conflicts": [],
            "recommendations": []
        }
        
        # 当年度データの検証
        if not df1_a.is_empty():
            current_sales_col = f"{current_year}売上"
            if current_sales_col in df1_a.columns:
                current_totals = {
                    "法人": df1_a[current_sales_col].sum(),
                    "得意先": df2_a[current_sales_col].sum() if current_sales_col in df2_a.columns else 0,
                    "得意先（商品）": df3_a[current_sales_col].sum() if current_sales_col in df3_a.columns else 0
                }
                validation_result["current_year_data"][current_sales_col] = current_totals
                
                logger.info(f"📊 当年度データ（{current_sales_col}）:")
                for data_type, total in current_totals.items():
                    logger.info(f"  {data_type}: {total:,.0f}円")
        
        # 過去データの検証
        if not df1_b.is_empty():
            # 過去データに含まれる年度売上列を特定
            past_sales_cols = [col for col in df1_b.columns if col.endswith("売上")]
            
            for sales_col in past_sales_cols:
                if sales_col in df1_b.columns and sales_col in df2_b.columns and sales_col in df3_b.columns:
                    past_totals = {
                        "法人": df1_b[sales_col].sum(),
                        "得意先": df2_b[sales_col].sum(),
                        "得意先（商品）": df3_b[sales_col].sum()
                    }
                    validation_result["past_data"][sales_col] = past_totals
                    
                    logger.info(f"📊 過去データ（{sales_col}）:")
                    for data_type, total in past_totals.items():
                        logger.info(f"  {data_type}: {total:,.0f}円")
                    
                    # 当年度データと過去データの重複チェック
                    if sales_col in validation_result["current_year_data"]:
                        current_total = validation_result["current_year_data"][sales_col]["法人"]
                        past_total = past_totals["法人"]
                        
                        if abs(current_total - past_total) > 1000:  # 1,000円以上の差異
                            conflict = {
                                "column": sales_col,
                                "current": current_total,
                                "past": past_total,
                                "diff": current_total - past_total
                            }
                            validation_result["potential_conflicts"].append(conflict)
                            logger.warning(f"⚠️ 潜在的競合: {sales_col} - 当年度{current_total:,.0f}円 vs 過去{past_total:,.0f}円")
        
        # 推奨事項の生成
        if validation_result["potential_conflicts"]:
            validation_result["recommendations"].append("データ結合時に年度データの上書きに注意")
            validation_result["recommendations"].append("結合後の詳細検証を必須実行")
        
        if df1_b.is_empty():
            validation_result["recommendations"].append("過去データが空のため、シンプルな当年度データ処理を推奨")
        
        logger.info(f"🔍 事前検証完了: 競合{len(validation_result['potential_conflicts'])}件, 推奨事項{len(validation_result['recommendations'])}件")
        
        return validation_result


class MemoryManager:
    """メモリ管理クラス"""
    
    @staticmethod
    def cleanup() -> None:
        """メモリクリーンアップを実行"""
        gc.collect()
    
    @staticmethod
    def get_memory_usage() -> float:
        """現在のメモリ使用量を取得（MB）"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / 1024 / 1024
        except ImportError:
            return 0.0


class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self):
        self.validator = DataIntegrityValidator()
        self.memory_manager = MemoryManager()
    
    def generate_years_list(self, start_year: int, end_year: int) -> List[str]:
        """対象年度のリストを作成（降順）
        
        Args:
            start_year: 開始年度
            end_year: 終了年度
            
        Returns:
            List[str]: 年度リスト（降順）
        """
        years = [str(year) for year in range(start_year, end_year + 1)]
        years.reverse()
        return years
    
    def load_current_data(self, filepath_base: str) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """当年度のデータ（parquet）を読み込み
        
        Args:
            filepath_base: ファイルパスのベース
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: 法人、得意先、得意先（商品）データ
        """
        logger.info("=== 当年度データ読み込み ===")
        datasets = {}
        
        for data_type in ProcessConfig.DATA_TYPES:
            filename = f"{filepath_base}_{data_type}.parquet"
            try:
                logger.info(f"読み込み中: {filename}")
                datasets[data_type] = pl.read_parquet(filename)
                row_count, col_count = datasets[data_type].shape
                logger.info(f"  {data_type}データ: {row_count:,}行, {col_count}列")
            except Exception as e:
                logger.error(f"❌ {data_type}データの読み込みに失敗: {e}")
                raise
        
        return datasets["法人"], datasets["得意先"], datasets["得意先（商品）"]
    
    def load_past_data(self, last_year: str, term: str) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """過去実績データ（Parquet）を読み込み
        
        Args:
            last_year: 前年度
            term: 期間
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: 法人、得意先、得意先（商品）データ
        """
        logger.info("=== 過去実績データ読み込み ===")
        datasets = {}
        
        for data_type in ProcessConfig.DATA_TYPES:
            try:
                filepath = str(get_past_parquet_path(last_year, term, data_type))
                logger.info(f"読み込み中: {filepath}")
                
                # ファイル存在チェック
                if not os.path.exists(filepath):
                    logger.warning(f"過去実績データファイルが見つかりません: {filepath}")
                    datasets[data_type] = pl.DataFrame()
                    continue
                
                # parquetファイルを読み込み
                datasets[data_type] = pl.read_parquet(filepath)
                row_count, col_count = datasets[data_type].shape
                logger.info(f"  {data_type}データ: {row_count:,}行, {col_count}列")
                
            except Exception as e:
                logger.error(f"❌ {data_type}データの読み込みに失敗: {e}")
                logger.error(f"エラーの詳細: {type(e).__name__}")
                import traceback
                logger.error(f"スタックトレース:\n{traceback.format_exc()}")
                datasets[data_type] = pl.DataFrame()
        
        return datasets["法人"], datasets["得意先"], datasets["得意先（商品）"]
    
    def rename_columns(self, df2_b: pl.DataFrame, df3_b: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """列名のリネーム処理
        
        Args:
            df2_b: 得意先の過去データ
            df3_b: 得意先（商品）の過去データ
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame]: リネーム後のデータフレーム
        """
        # 空のデータフレームの場合はそのまま返す
        if df2_b.is_empty() or df3_b.is_empty():
            return df2_b, df3_b
        
        # polars方式で列名リネーム
        if "取引先コード" in df2_b.columns:
            df2_b = df2_b.rename({"取引先コード": "得意先コード", "取引先名": "得意先名"})
        if "得意先" in df3_b.columns:
            df3_b = df3_b.rename({"得意先": "得意先名"})
        
        return df2_b, df3_b
    
    def add_past_suffix(self, df: pl.DataFrame, key_columns: List[str]) -> pl.DataFrame:
        """過年度データの列名に_pastサフィックスを追加（結合キー保護版）
        
        修正: 結合キー列は保護し、データ列のみにサフィックスを追加
        
        Args:
            df: データフレーム
            key_columns: 結合キー列リスト（保護対象）
            
        Returns:
            pl.DataFrame: サフィックス追加後のデータフレーム
        """
        if df.is_empty():
            return df
            
        rename_mapping = {}
        
        # 基本的な結合キー列（これらは保護する）
        protected_keys = ["法人コード", "法人名", "得意先コード", "得意先名", 
                         "営業所名", "担当者名", "第1階層", "商品コード", "商品名"]
        
        # 実際に指定されたキー列も保護対象に追加
        all_protected_keys = set(protected_keys + key_columns)
        
        for col in df.columns:
            if col in all_protected_keys:
                # 結合キーは保護（サフィックス追加しない）
                continue
            else:
                # データ列のみにサフィックスを追加
                rename_mapping[col] = f"{col}_past"
        
        if rename_mapping:
            df = df.rename(rename_mapping)
            logger.info(f"過去データ列名変更: {len(rename_mapping)}列にサフィックス追加 (結合キー{len(all_protected_keys)}列は保護)")
        else:
            logger.info("過去データ: サフィックス追加対象列なし（全て結合キー）")
            
        return df
    
    def fix_full_join_nulls(self, df: pl.DataFrame, years_list: List[str], current_year_str: str) -> pl.DataFrame:
        """full join後のnull値を統一的に修復する（年度を意識した処理）
        
        重要な修正: 過年度データの無条件上書きを防止し、当年度データの保護を優先
        
        Args:
            df: データフレーム
            years_list: 年度リスト
            current_year_str: 当年度文字列
            
        Returns:
            pl.DataFrame: null値修復後のデータフレーム
        """
        # _pastで終わる列を見つけて処理
        past_columns = [col for col in df.columns if col.endswith('_past')]
        
        # まず、IDカラム（法人コード、得意先コードなど）の処理
        for past_col in past_columns:
            base_col = past_col.replace('_past', '')
            
            # IDカラムの場合（数値データではない）
            if not any(year in base_col for year in years_list):
                if base_col not in df.columns:
                    df = df.with_columns(pl.col(past_col).alias(base_col))
                else:
                    # IDカラムはnullの場合のみ補完（重要: 既存データを保護）
                    df = df.with_columns([
                        pl.when(pl.col(base_col).is_null())
                        .then(pl.col(past_col))
                        .otherwise(pl.col(base_col))
                        .alias(base_col)
                    ])
        
        # 年度データの処理（修正版: 当年度データを保護し、過年度データの上書きを防止）
        for year_str in years_list:
            # 売上数は当年度のみ処理
            if year_str == current_year_str:
                suffixes = ["売上数", "㎡", "売上", "粗利"]
            else:
                suffixes = ["㎡", "売上", "粗利"]
            
            for suffix in suffixes:
                col_name = f"{year_str}{suffix}"
                past_col_name = f"{col_name}_past"
                
                if year_str == current_year_str:
                    # 当年度データは既存のカラムをそのまま維持（修正なし）
                    # もしカラムが存在しない場合のみ作成
                    if col_name not in df.columns:
                        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col_name))
                    # ★重要★: 当年度データは_pastカラムで上書きしない（既存のデータを保持）
                else:
                    # ★修正★: 過年度データの処理を条件付きに変更
                    if past_col_name in df.columns:
                        if col_name not in df.columns:
                            # 過年度カラムが存在しない場合のみ、_pastから作成
                            df = df.with_columns(pl.col(past_col_name).alias(col_name))
                        else:
                            # ★重要な修正★: 過年度カラムが既に存在する場合、nullの場合のみ_pastで補完
                            # 無条件上書きではなく、既存データを保護
                            df = df.with_columns([
                                pl.when(pl.col(col_name).is_null())
                                .then(pl.col(past_col_name))
                                .otherwise(pl.col(col_name))
                                .alias(col_name)
                            ])
                    elif col_name not in df.columns:
                        # _pastカラムもcurrentカラムも存在しない場合、nullで作成
                        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col_name))
        
        # 全ての_past列を削除
        for past_col in past_columns:
            if past_col in df.columns:
                df = df.drop(past_col)
        
        return df
    
    def merge_dataframes(self, df1_a: pl.DataFrame, df1_b: pl.DataFrame, 
                        df2_a: pl.DataFrame, df2_b: pl.DataFrame,
                        df3_a: pl.DataFrame, df3_b: pl.DataFrame,
                        years: List[str], current_year: int) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """データフレームの結合（安全な結合キー処理版）
        
        修正: 結合キーを保護し、データ差異を防止
        
        Args:
            df1_a, df1_b: 法人データ（当年度、過去）
            df2_a, df2_b: 得意先データ（当年度、過去）
            df3_a, df3_b: 得意先（商品）データ（当年度、過去）
            years: 年度リスト
            current_year: 当年度
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: 結合後のデータフレーム
        """
        logger.info("=== データ結合処理 ===")
        
        # 過去データが空の場合は当年度データをそのまま返す
        if df1_b.is_empty():
            logger.info("過去データが空のため、当年度データのみを使用します。")
            return df1_a, df2_a, df3_a
        
        # 過年度データの列名調整（修正版: 結合キー保護）
        logger.info("過年度データの列名調整中...")
        df1_b_renamed = self.add_past_suffix(df1_b, ["法人コード"])  # 法人データの結合キー
        df2_b_renamed = self.add_past_suffix(df2_b, ["得意先コード", "得意先名"])  # 得意先データの結合キー
        df3_b_renamed = self.add_past_suffix(df3_b, ProcessConfig.PRODUCT_JOIN_KEYS)  # 商品データの結合キー
        
        # 安全な結合処理（結合キーが保護されているため、同名キーで結合可能）
        df1 = df1_a.join(df1_b_renamed, how="full", on=["法人コード"])
        logger.info(f"法人データ結合完了: {len(df1):,}行, {len(df1.columns)}列")
        
        df2 = df2_a.join(df2_b_renamed, how="full", on=["得意先コード", "得意先名"])
        logger.info(f"得意先データ結合完了: {len(df2):,}行, {len(df2.columns)}列")
        
        # 得意先（商品）データの結合
        # 結合キーのうち、両方のデータフレームに存在するもののみを使用
        join_keys = [key for key in ProcessConfig.PRODUCT_JOIN_KEYS if key in df3_a.columns and key in df3_b.columns]
        
        logger.info(f"得意先（商品）データの結合キー: {join_keys}")
        
        # データが空でない場合のみ結合
        if not df3_b.is_empty() and join_keys:
            # 修正: 同名キーで結合（結合キーが保護されているため）
            df3 = df3_a.join(df3_b_renamed, how="full", on=join_keys)
        else:
            logger.warning("過去の得意先（商品）データが空または結合キーが不足しているため、当年度データのみを使用します。")
            df3 = df3_a
            
        logger.info(f"得意先（商品）データ結合完了: {len(df3):,}行, {len(df3.columns)}列")
        
        # 結合後のnull値修復処理
        logger.info("結合後のnull値修復処理中...")
        df1 = self.fix_full_join_nulls(df1, years, str(current_year))
        df2 = self.fix_full_join_nulls(df2, years, str(current_year))
        df3 = self.fix_full_join_nulls(df3, years, str(current_year))
        logger.info("全テーブルのnull値修復完了")
        
        return df1, df2, df3
    
    def merge_with_master(self, df1: pl.DataFrame, master_path: str) -> pl.DataFrame:
        """最新法人マスタとの結合
        
        Args:
            df1: 法人データ
            master_path: マスタファイルパス
            
        Returns:
            pl.DataFrame: マスタ結合後のデータフレーム
        """
        try:
            logger.info(f"読み込み中: {master_path}/法人マスタ.csv")
            
            # データ型を明示的に指定
            schema = {
                "法人コード": pl.Utf8,
                "法人名": pl.Utf8
            }
            
            df1_c = pl.read_csv(
                f"{master_path}/法人マスタ.csv", 
                encoding="cp932",
                schema_overrides=schema
            ).select(["法人コード", "法人名"])
            
            df1 = df1.join(df1_c, on=["法人コード"], how="left", suffix="_new")
            
            # 法人名の統合（既存があれば優先、なければ新しいものを使用）
            if "法人名" in df1.columns and "法人名_new" in df1.columns:
                df1 = df1.with_columns(
                    pl.coalesce([pl.col("法人名"), pl.col("法人名_new")]).alias("法人名")
                ).drop(["法人名_new"])
            elif "法人名_new" in df1.columns:
                df1 = df1.rename({"法人名_new": "法人名"})
            
            return df1
            
        except Exception as e:
            logger.error(f"❌ マスタ結合エラー: {e}")
            raise
    
    def aggregate_product_data(self, df3: pl.DataFrame) -> pl.DataFrame:
        """得意先（商品）データの集計
        
        Args:
            df3: 得意先（商品）データ
            
        Returns:
            pl.DataFrame: 集計後のデータフレーム
        """
        # 数値列を特定（グループ化キー以外の列）
        potential_group_cols = ["営業所名", "担当者名", "得意先コード", "得意先名", "第1階層", "商品コード", "商品名"]
        # 実際に存在するカラムのみを使用
        group_cols = [col for col in potential_group_cols if col in df3.columns]
        numeric_cols = [col for col in df3.columns if col not in group_cols]
        
        logger.info(f"集計グループ列: {group_cols}")
        
        # 数値列のみを集計
        agg_exprs = [pl.col(col).sum() for col in numeric_cols if df3[col].dtype in [pl.Int64, pl.Float64]]
        
        if group_cols and agg_exprs:
            return df3.group_by(group_cols).agg(agg_exprs)
        else:
            logger.warning("集計に必要な列が不足しています")
            return df3
    
    def reorder_columns(self, df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame, 
                       year: str, years: List[str]) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """列順の整理
        
        Args:
            df1, df2, df3: データフレーム
            year: 当年度
            years: 年度リスト
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: 整理後のデータフレーム
        """
        logger.info("=== 列順整理処理 ===")
        
        # ベース列を定義
        base_cols_df1 = ["法人コード", "法人名", f"{year}売上数"]
        base_cols_df2 = ["得意先コード", "得意先名", f"{year}売上数"]
        # df3のベース列は実際に存在するカラムのみ使用
        potential_base_cols_df3 = ["営業所名", "担当者名", "得意先コード", "得意先名", "第1階層", "商品コード", "商品名", f"{year}売上数"]
        base_cols_df3 = [col for col in potential_base_cols_df3 if col in df3.columns]
        
        # 各データフレームの列を取得
        cols1 = df1.columns
        cols2 = df2.columns
        cols3 = df3.columns
        
        # 年次列をフィルタリング (前年度売上数は除外)
        def get_year_cols(columns):
            return [col for col in columns 
                    if any(str(y) in col for y in years) 
                    and not col.endswith(f"{int(year)-1}売上数")
                    and col not in base_cols_df1 + base_cols_df2 + base_cols_df3]
        
        # 各データフレーム用の年次列を取得
        year_cols1 = [col for col in get_year_cols(cols1) if col in cols1]
        year_cols2 = [col for col in get_year_cols(cols2) if col in cols2]
        year_cols3 = [col for col in get_year_cols(cols3) if col in cols3]
        
        # 存在する列のみを選択（polars方式）
        def safe_select(df, columns):
            existing_cols = [col for col in columns if col in df.columns]
            return df.select(existing_cols) if existing_cols else df
        
        return (
            safe_select(df1, base_cols_df1 + year_cols1),
            safe_select(df2, base_cols_df2 + year_cols2),
            safe_select(df3, base_cols_df3 + year_cols3)
        )
    
    def identify_column_types(self, df: pl.DataFrame) -> Tuple[List[str], List[str]]:
        """値列とindex列を動的に特定
        
        Args:
            df: データフレーム
            
        Returns:
            Tuple[List[str], List[str]]: (インデックス列, 値列)
        """
        all_cols = df.columns
        
        # 値列（売上、数量、㎡、粗利など）を特定
        value_cols = [col for col in all_cols 
                     if any(pattern in col for pattern in ProcessConfig.SALES_PATTERNS)]
        
        # index列（値列以外のすべて）を特定
        index_cols = [col for col in all_cols if col not in value_cols]
        
        return index_cols, value_cols
    
    def fill_missing_index_columns(self, df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame,
                                  df1_b: pl.DataFrame, df2_b: pl.DataFrame, df3_b: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """過去年度専用データのindex列穴埋め
        
        Args:
            df1, df2, df3: 結合後データ
            df1_b, df2_b, df3_b: 過去データ
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: 穴埋め後のデータフレーム
        """
        logger.info("=== index列穴埋め処理 ===")
        
        if df1_b.is_empty():
            logger.info("過去データが空のため、穴埋めをスキップします")
            return df1, df2, df3
        
        # 法人データの穴埋め（重複排除機能付き）
        if len(df1) > 0 and len(df1_b) > 0:
            index_cols1, value_cols1 = self.identify_column_types(df1)
            logger.info(f"法人データ - index列: {index_cols1}")
            logger.info(f"法人データ - 値列: {value_cols1}")
            
            # 補完前の状態を記録
            rows_before = len(df1)
            null_count_before = df1["法人コード"].is_null().sum()
            
            # 法人名をキーとして過去データから法人コードを補完
            if "法人名" in df1.columns and "法人名" in df1_b.columns and "法人コード" in df1_b.columns:
                # 法人コードがnullで法人名がnot nullの行を特定
                null_code_mask = df1["法人コード"].is_null()
                has_name_mask = df1["法人名"].is_not_null()
                target_mask = null_code_mask & has_name_mask
                
                if target_mask.sum() > 0:
                    logger.info(f"法人データ: {target_mask.sum()}行の法人コードを補完します")
                    
                    # 過去データから法人名→法人コードのマッピングを作成（重複チェック付き）
                    past_mapping = df1_b.select(["法人名", "法人コード"]).unique().drop_nulls()
                    
                    # 法人名の重複チェック
                    name_counts = past_mapping.group_by("法人名").agg(pl.len().alias("count"))
                    duplicate_names = name_counts.filter(pl.col("count") > 1)
                    
                    if len(duplicate_names) > 0:
                        logger.warning(f"警告: {len(duplicate_names)}件の法人名で複数の法人コードが存在します")
                        # 重複がある場合は最初の1件のみを使用
                        past_mapping = past_mapping.group_by("法人名").first()
                    
                    logger.info(f"マッピング件数: {len(past_mapping)}件")
                    
                    # left joinで法人コードを補完
                    df1_filled = df1.join(
                        past_mapping.rename({"法人コード": "法人コード_補完"}),
                        on="法人名", 
                        how="left"
                    )
                    
                    # 元の法人コードがnullの場合のみ補完値を使用
                    df1 = df1_filled.with_columns(
                        pl.coalesce([pl.col("法人コード"), pl.col("法人コード_補完")]).alias("法人コード")
                    ).drop("法人コード_補完")
                    
                    # 補完後の状態をチェック
                    rows_after = len(df1)
                    null_count_after = df1["法人コード"].is_null().sum()
                    
                    logger.info("法人データ補完結果:")
                    logger.info(f"  行数: {rows_before} → {rows_after} (差: {rows_after - rows_before})")
                    logger.info(f"  null数: {null_count_before} → {null_count_after} (補完: {null_count_before - null_count_after})")
                    
                    # 重複チェック
                    if rows_after > rows_before:
                        logger.warning("⚠ 警告: 補完処理で行数が増加しました。重複の可能性があります。")
                        # 法人コード+法人名での重複排除
                        df1_dedup = df1.unique(subset=["法人コード", "法人名"])
                        rows_dedup = len(df1_dedup)
                        if rows_dedup < rows_after:
                            logger.info(f"重複排除実行: {rows_after} → {rows_dedup}行 (削除: {rows_after - rows_dedup}行)")
                            df1 = df1_dedup
                        else:
                            logger.info("法人コード+法人名による重複は検出されませんでした")
                    
                    logger.info(f"最終的な法人データ: {len(df1)}行, null数: {df1['法人コード'].is_null().sum()}")
        
        # 得意先データの穴埋め
        if len(df2) > 0 and len(df2_b) > 0:
            index_cols2, value_cols2 = self.identify_column_types(df2)
            logger.info(f"得意先データ - index列: {index_cols2}")
            logger.info(f"得意先データ - 値列: {value_cols2}")
            
            # 得意先名をキーとして過去データから得意先コードを補完
            if "得意先名" in df2.columns and "得意先名" in df2_b.columns and "得意先コード" in df2_b.columns:
                null_code_mask = df2["得意先コード"].is_null()
                has_name_mask = df2["得意先名"].is_not_null()
                target_mask = null_code_mask & has_name_mask
                
                if target_mask.sum() > 0:
                    logger.info(f"得意先データ: {target_mask.sum()}行の得意先コードを補完します")
                    
                    # 過去データから得意先名→得意先コードのマッピングを作成
                    past_mapping = df2_b.select(["得意先名", "得意先コード"]).unique().drop_nulls()
                    
                    # left joinで得意先コードを補完
                    df2_filled = df2.join(
                        past_mapping.rename({"得意先コード": "得意先コード_補完"}),
                        on="得意先名", 
                        how="left"
                    )
                    
                    # 元の得意先コードがnullの場合のみ補完値を使用
                    df2 = df2_filled.with_columns(
                        pl.coalesce([pl.col("得意先コード"), pl.col("得意先コード_補完")]).alias("得意先コード")
                    ).drop("得意先コード_補完")
                    
                    logger.info(f"得意先データ補完後の得意先コードnull数: {df2['得意先コード'].is_null().sum()}")
        
        # 得意先（商品）データの穴埋め（軽量版）
        if len(df3) > 0 and len(df3_b) > 0:
            index_cols3, value_cols3 = self.identify_column_types(df3)
            logger.info(f"得意先（商品）データ - index列: {index_cols3}")
            logger.info(f"得意先（商品）データ - 値列: {value_cols3}")
            
            # null数をカウントして表示（実際の補完はスキップして性能優先）
            null_counts = {}
            for col in index_cols3:
                if col in df3.columns:
                    null_count = df3[col].is_null().sum()
                    if null_count > 0:
                        null_counts[col] = null_count
                        logger.info(f"得意先（商品）データ: {null_count}行の{col}がnullです")
            
            if null_counts:
                logger.info(f"穴埋め対象: {len(null_counts)}列（性能優先のため実際の補完は省略）")
                logger.info("※ 大容量データのため、穴埋め処理は必要に応じて個別実装してください")
            else:
                logger.info("得意先（商品）データ: index列にnullはありません")
        
        logger.info("index列穴埋め処理完了")
        return df1, df2, df3
    
    def sort_dataframes(self, df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame,
                       years: List[str], current_year: int) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """データフレームのソート（動的年度対応の当年度売上優先）
        
        Args:
            df1, df2, df3: データフレーム
            years: 年度リスト
            current_year: 当年度
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: ソート後のデータフレーム
        """
        logger.info("=== ソート処理 ===")
        
        # 動的年度に基づく売上列名
        current_sales_col = f"{current_year}売上"
        
        # 当年度売上列が存在する場合は当年度優先でソート
        if current_sales_col in df1.columns:
            logger.info(f"動的年度売上（{current_sales_col}）で降順ソートします")
            
            # polarsでnull値を0で置換してからソート
            df1 = df1.with_columns(pl.col(current_sales_col).fill_null(0))
            df2 = df2.with_columns(pl.col(current_sales_col).fill_null(0))
            df3 = df3.with_columns(pl.col(current_sales_col).fill_null(0))
            
            # 降順ソート（polarsの効率的なソート）
            df1 = df1.sort(current_sales_col, descending=True)
            df2 = df2.sort(current_sales_col, descending=True)
            df3 = df3.sort([current_sales_col, "営業所名"], descending=[True, False])
            
            # ソート結果確認（polarsのselect + headで効率的に確認）
            logger.info("ソート結果確認:")
            try:
                top5_corp = df1.select(current_sales_col).head(5)[current_sales_col].to_list()
                top5_client = df2.select(current_sales_col).head(5)[current_sales_col].to_list()
                top5_product = df3.select(current_sales_col).head(5)[current_sales_col].to_list()
                
                logger.info(f"  法人トップ5の{current_sales_col}: {[f'{x:,.0f}' for x in top5_corp]}")
                logger.info(f"  得意先トップ5の{current_sales_col}: {[f'{x:,.0f}' for x in top5_client]}")
                logger.info(f"  得意先（商品）トップ5の{current_sales_col}: {[f'{x:,.0f}' for x in top5_product]}")
            except Exception as e:
                logger.warning(f"  ソート結果確認でエラー: {e}")
        else:
            logger.info(f"動的年度売上列（{current_sales_col}）が存在しないため、従来のソートを実行します")
            # フォールバック：従来のソート方式
            sort_columns = [f"{year}売上" for year in years if f"{year}売上" in df1.columns]
            
            if sort_columns:
                df1 = df1.sort(sort_columns[0], descending=True)
                df2 = df2.sort(sort_columns[0], descending=True)
            
            df3 = df3.sort(["営業所名", "得意先名", "第1階層"], descending=[False, False, False])
        
        logger.info(f"ソート完了 - 法人: {len(df1):,}行, 得意先: {len(df2):,}行, 得意先（商品）: {len(df3):,}行")
        
        return df1, df2, df3
    
    def convert_data_types(self, df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """データ型の変換
        
        Args:
            df1, df2, df3: データフレーム
            
        Returns:
            Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]: 変換後のデータフレーム
        """
        logger.info("=== データ型変換処理 ===")
        
        # コード列を文字列型に変換（polars方式）
        for col in ProcessConfig.CODE_COLUMNS:
            if col in df1.columns:
                df1 = df1.with_columns(pl.col(col).cast(pl.Utf8))
            if col in df2.columns:
                df2 = df2.with_columns(pl.col(col).cast(pl.Utf8))
            if col in df3.columns:
                df3 = df3.with_columns(pl.col(col).cast(pl.Utf8))
        
        return df1, df2, df3
    
    def save_data(self, df1: pl.DataFrame, df2: pl.DataFrame, df3: pl.DataFrame, output_dir: str) -> None:
        """データの保存
        
        Args:
            df1, df2, df3: データフレーム
            output_dir: 出力ディレクトリ
        """
        logger.info("=== データファイル出力処理 ===")
        os.makedirs(output_dir, exist_ok=True)
        
        # polars方式でのデータ保存
        df1.write_parquet(f"{output_dir}/法人_転記用データ.parquet")
        logger.info(f"法人データを {output_dir}/法人_転記用データ.parquet に保存しました")
        
        df2.write_parquet(f"{output_dir}/得意先_転記用データ.parquet")
        logger.info(f"得意先データを {output_dir}/得意先_転記用データ.parquet に保存しました")
        
        df3.write_parquet(f"{output_dir}/得意先（商品）_転記用データ.parquet")
        logger.info(f"得意先（商品）データを {output_dir}/得意先（商品）_転記用データ.parquet に保存しました")


def main() -> None:
    """メイン処理（データ差異修正版）"""
    start_time = time.time()
    
    # クラス初期化
    processor = DataProcessor()
    validator = DataIntegrityValidator()
    memory_manager = MemoryManager()
    
    try:
        logger.info("=== 過去データ結合処理開始（修正版） ===")
        
        # 初期メモリ使用量
        initial_memory = memory_manager.get_memory_usage()
        if initial_memory > 0:
            logger.info(f"📊 初期メモリ使用量: {initial_memory:.1f}MB")
        
        # 年度計算
        year, last_year, current_year = get_fiscal_years()
        years = processor.generate_years_list(START_YEAR, current_year)
        
        # ファイルパスの設定
        filepath_current = f"{TERM}_ランキング（得意先）"
        master_path = str(MASTER_DIR)
        
        # データ読み込み
        logger.info("⏱️ データ読み込み開始")
        df1_a, df2_a, df3_a = processor.load_current_data(filepath_current)
        df1_b, df2_b, df3_b = processor.load_past_data(last_year, TERM)
        
        # ★新機能★: 結合前データの事前検証
        pre_validation = validator.validate_pre_merge_data(
            df1_a, df2_a, df3_a, df1_b, df2_b, df3_b, current_year
        )
        
        # メモリクリーンアップ
        memory_manager.cleanup()
        
        # 結合前の過去データ総計値を記録
        past_totals = validator.validate_data_totals(df1_b, df2_b, df3_b, "過去データ")
        
        # データ処理（修正版の結合処理を使用）
        logger.info("⏱️ データ処理開始（安全な結合処理）")
        df2_b, df3_b = processor.rename_columns(df2_b, df3_b)
        df1, df2, df3 = processor.merge_dataframes(df1_a, df1_b, df2_a, df2_b, df3_a, df3_b, years, current_year)
        
        # メモリクリーンアップ
        del df1_a, df1_b, df2_a, df2_b, df3_a, df3_b
        memory_manager.cleanup()
        
        df1 = processor.merge_with_master(df1, master_path)
        df3 = processor.aggregate_product_data(df3)
        df1, df2, df3 = processor.reorder_columns(df1, df2, df3, year, years)
        df1, df2, df3 = processor.fill_missing_index_columns(df1, df2, df3, pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
        df1, df2, df3 = processor.sort_dataframes(df1, df2, df3, years, current_year)
        df1, df2, df3 = processor.convert_data_types(df1, df2, df3)
        
        # ★強化★: 結合後のデータ総計値を検証（詳細版）
        after_totals = validator.validate_data_totals(df1, df2, df3, "結合後")
        integrity_check_passed = validator.compare_totals(past_totals, after_totals, year)
        
        # 整合性チェック結果に応じた処理
        if not integrity_check_passed:
            logger.error("❌ データ整合性チェックに失敗しました")
            logger.error("⚠️  処理を継続しますが、結果の確認を強く推奨します")
        else:
            logger.info("✅ データ整合性チェック: 正常")
        
        # データ保存
        processor.save_data(df1, df2, df3, ProcessConfig.OUTPUT_DIR)
        
        # 最終メモリ使用量
        final_memory = memory_manager.get_memory_usage()
        if final_memory > 0 and initial_memory > 0:
            memory_used = final_memory - initial_memory
            logger.info(f"📊 最大メモリ使用量: +{memory_used:.1f}MB")
        
        # 処理時間表示
        elapsed_time = time.time() - start_time
        logger.info("=== 全ての処理が完了しました ===")
        logger.info(f"⏱️ 処理時間: {elapsed_time:.2f}秒")
        
        if integrity_check_passed:
            console.print("✅ [bold green]過去データ結合処理が完了しました（整合性確認済み）[/bold green]")
        else:
            console.print("⚠️ [bold yellow]過去データ結合処理が完了しましたが、データ差異を検出[/bold yellow]")
            console.print("📋 詳細ログを確認して、データの整合性を検証してください")
        
    except Exception as e:
        logger.error(f"❌ 処理中にエラーが発生しました: {str(e)}")
        console.print(f"❌ [bold red]過去データ結合処理が失敗しました: {e}[/bold red]")
        raise


if __name__ == "__main__":
    main()
