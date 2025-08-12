import datetime
import time
import warnings
import os
import platform
from typing import List, Tuple, Optional
import polars as pl
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# 警告を抑制
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl.worksheet.header_footer")

class DataProcessor:
    """データ処理クラス（polars最適化版）"""
    
    def __init__(self):
        # 定数定義
        self.TERM = "2Q"
        self.TERM_JP = "第２四半期"
        self.START_YEAR_DATA = 2015
        
        # 年度設定
        self.processing_year, self.last_processing_year = self._calculate_fiscal_years()
        self.years_list = self._generate_years_list()
        self.current_year_str = str(self.processing_year)
        self.last_year_str = str(self.last_processing_year)
        
        # ファイルパス設定
        self._setup_file_paths()
    
    def _calculate_fiscal_years(self) -> Tuple[int, int]:
        """会計年度を計算"""
        now = datetime.datetime.now()
        current_system_year = now.year
        
        if now.month >= 4:
            processing_year = current_system_year
        else:
            processing_year = current_system_year - 1
            
        return processing_year, processing_year - 1
    
    def _generate_years_list(self) -> List[str]:
        """処理対象年度のリストを生成（降順）"""
        years_list = [str(y) for y in range(self.START_YEAR_DATA, self.processing_year + 1)]
        years_list.reverse()
        return years_list
    
    def _setup_file_paths(self):
        """ファイルパスを設定"""
        self.current_year_data_prefix = f"{self.TERM}_ランキング（得意先）"
        # 過去データ参照先をExcelからparquetディレクトリに変更
        self.past_parquet_dir = f"D:/Skit_Actual/PG/累計/昨年度データ/parquet"
        self.past_parquet_paths = {
            "法人": f"{self.past_parquet_dir}/第２四半期_ランキング（得意先）_法人.parquet",
            "得意先": f"{self.past_parquet_dir}/第２四半期_ランキング（得意先）_得意先.parquet",
            "得意先（商品）": f"{self.past_parquet_dir}/第２四半期_ランキング（得意先）_得意先（商品）.parquet"
        }
        self.master_filepath = "../../マスタ"
        self.output_dir = "転記用データ"
    
    
    def _log_dataframe_info(self, df: pl.DataFrame, df_name: str):
        """データフレームの情報を整形してログ出力するヘルパー関数"""
        print(f"\n--- DataFrame Info: {df_name} ---")
        print(f"  Rows: {len(df):,}")
        print(f"  Columns: {len(df.columns)}")
        print("  Schema:")
        for col in df.columns:
            col_type = str(df[col].dtype)
            null_count = df[col].is_null().sum()
            print(f"    - {col}: {col_type}, Nulls: {null_count} ({null_count / len(df) * 100:.2f}%)")
        print("----------------------------------")

    def load_current_year_data(self) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """当該年度のParquetデータを読み込み（polars使用で高速化）"""
        print("=== 当該年度データ読み込み（polars版） ===")
        
        files = [
            (f"{self.current_year_data_prefix}_法人.parquet", "法人"),
            (f"{self.current_year_data_prefix}_得意先.parquet", "得意先"),
            (f"{self.current_year_data_prefix}_得意先（商品）.parquet", "得意先（商品）")
        ]
        
        def load_parquet_file(file_info):
            filepath, name = file_info
            print(f"読み込み中: {filepath}")
            try:
                # polarsの高速parquet読み込み
                df = pl.read_parquet(filepath)
                print(f"{name}データ読み込み完了: {len(df):,}行")
                return df
            except FileNotFoundError:
                print(f"ファイルが見つかりません: {filepath}")
                raise
        
        # 並列読み込みで高速化
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(load_parquet_file, file_info) for file_info in files]
            dataframes = [future.result() for future in futures]
        
                
        return tuple(dataframes)
    
    def load_past_year_data(self) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """過年度のParquetデータを読み込み"""
        print("\n=== 過年度データ読み込み（Parquet版） ===")
        
        print(f"過年度データParquetディレクトリ: {self.past_parquet_dir}")
        
        # ディレクトリ存在チェック
        if not os.path.exists(self.past_parquet_dir):
            print(f"[ERROR] Parquetディレクトリが存在しません: {self.past_parquet_dir}")
            raise FileNotFoundError(f"過年度データParquetディレクトリが存在しません: {self.past_parquet_dir}")
        
        datasets = {}
        data_types = ["法人", "得意先", "得意先（商品）"]
        
        print(f"対象年度: {self.last_year_str}年度, 期間: {self.TERM_JP}")
        print(f"読み込み予定ファイル:")
        
        # 全ファイルの存在確認を事前に実施
        all_files_exist = True
        for data_type in data_types:
            parquet_path = self.past_parquet_paths[data_type]
            print(f"  - {data_type}: {parquet_path}")
            if not os.path.exists(parquet_path):
                print(f"[ERROR] ファイル不存在: {parquet_path}")
                all_files_exist = False
        
        if not all_files_exist:
            print("[ERROR] 一部のParquetファイルが存在しません")
            raise FileNotFoundError(f"一部のParquetファイルが存在しません")
        
        try:
            print("[SUCCESS] 全てのParquetファイルが確認できました。読み込み開始...")
            
            for data_type in data_types:
                parquet_path = self.past_parquet_paths[data_type]
                
                try:
                    print(f"[LOADING] {data_type}データ読み込み中: {os.path.basename(parquet_path)}")
                    
                    # ファイルサイズ情報も表示
                    file_size = os.path.getsize(parquet_path) / (1024 * 1024)  # MB
                    print(f"   ファイルサイズ: {file_size:.1f}MB")
                    
                    datasets[data_type] = pl.read_parquet(parquet_path)
                    print(f"   [SUCCESS] 読み込み完了: {len(datasets[data_type])}行, {len(datasets[data_type].columns)}列")
                    
                except Exception as e:
                    print(f"[ERROR] {data_type}データのParquet読み込みエラー: {e}")
                    print(f"   ファイル: {parquet_path}")
                    raise Exception(f"{data_type}データの読み込みに失敗しました")
        
        except Exception as e:
            print(f"[ERROR] Parquet読み込み処理で予期しないエラー: {e}")
            raise
        
        print("[SUCCESS] Parquetファイルからの読み込みが正常に完了しました")
        return datasets["法人"], datasets["得意先"], datasets["得意先（商品）"]
    
    def preprocess_past_data(self, df2_past: pl.DataFrame, df3_past: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        """過年度データの前処理（polars版）"""
        print("\n=== 過年度データ前処理 ===")
        
        # 列名変更（存在する列のみ）
        print(f"得意先データの列: {df2_past.columns}")
        
        # 得意先データの列名変更
        rename_mapping_df2 = {}
        if "取引先コード" in df2_past.columns:
            rename_mapping_df2["取引先コード"] = "得意先コード"
        if "取引先名" in df2_past.columns:
            rename_mapping_df2["取引先名"] = "得意先名"
        
        if rename_mapping_df2:
            df2_past = df2_past.rename(rename_mapping_df2)
            print(f"得意先データ列名変更: {rename_mapping_df2}")
        
        # 得意先商品データの列名変更
        print(f"得意先商品データの列: {df3_past.columns}")
        
        rename_mapping_df3 = {}
        if "得意先" in df3_past.columns:
            rename_mapping_df3["得意先"] = "得意先名"
        
        if rename_mapping_df3:
            df3_past = df3_past.rename(rename_mapping_df3)
            print(f"得意先商品データ列名変更: {rename_mapping_df3}")
        
        print("列名変更完了")
        
        
        return df2_past, df3_past
    
    def get_yearly_values_before_merge(self, current_data: Tuple, past_data: Tuple) -> dict:
        """結合前データの各年度値を取得するメソッド"""
        print("\n=== 結合前データの年度値取得 ===")
        
        df1_current, df2_current, df3_current = current_data
        df1_past, df2_past, df3_past = past_data
        
        yearly_values = {
            "current": {},
            "past": {},
            "all_years": {}
        }
        
        # 当該年度データの値を取得（データが存在する場合のみ）
        if df1_current is not None and df2_current is not None and df3_current is not None:
            for year_str in self.years_list:
                if year_str == self.current_year_str:
                    suffixes = ["売上数", "㎡", "売上", "粗利"]
                else:
                    suffixes = ["㎡", "売上", "粗利"]
                
                yearly_values["current"][year_str] = {}
                for suffix in suffixes:
                    col_name = f"{year_str}{suffix}"
                    if col_name in df1_current.columns:
                        # 法人データの合計値を取得
                        total_value = df1_current.select(pl.col(col_name).sum()).item()
                        yearly_values["current"][year_str][f"法人_{suffix}"] = total_value
                        
                        # 得意先データの合計値を取得
                        if col_name in df2_current.columns:
                            total_value2 = df2_current.select(pl.col(col_name).sum()).item()
                            yearly_values["current"][year_str][f"得意先_{suffix}"] = total_value2
                        
                        # 得意先商品データの合計値を取得
                        if col_name in df3_current.columns:
                            total_value3 = df3_current.select(pl.col(col_name).sum()).item()
                            yearly_values["current"][year_str][f"得意先商品_{suffix}"] = total_value3
        
        # 過年度データの値を取得（データが存在する場合のみ）
        if df1_past is not None and df2_past is not None and df3_past is not None:
            for year_str in self.years_list:
                if year_str != self.current_year_str:  # 過年度のみ
                    suffixes = ["㎡", "売上", "粗利"]
                    
                    yearly_values["past"][year_str] = {}
                    for suffix in suffixes:
                        col_name = f"{year_str}{suffix}"
                        if col_name in df1_past.columns:
                            # 法人データの合計値を取得
                            total_value = df1_past.select(pl.col(col_name).sum()).item()
                            yearly_values["past"][year_str][f"法人_{suffix}"] = total_value
                            
                            # 得意先データの合計値を取得
                            if col_name in df2_past.columns:
                                total_value2 = df2_past.select(pl.col(col_name).sum()).item()
                                yearly_values["past"][year_str][f"得意先_{suffix}"] = total_value2
                            
                            # 得意先商品データの合計値を取得
                            if col_name in df3_past.columns:
                                total_value3 = df3_past.select(pl.col(col_name).sum()).item()
                                yearly_values["past"][year_str][f"得意先商品_{suffix}"] = total_value3
        
        # 全年度の値を統合
        for year_str in self.years_list:
            yearly_values["all_years"][year_str] = {}
            
            if year_str == self.current_year_str:
                suffixes = ["売上数", "㎡", "売上", "粗利"]
            else:
                suffixes = ["㎡", "売上", "粗利"]
            
            for suffix in suffixes:
                key = f"法人_{suffix}"
                if year_str in yearly_values["current"] and key in yearly_values["current"][year_str]:
                    yearly_values["all_years"][year_str][key] = yearly_values["current"][year_str][key]
                elif year_str in yearly_values["past"] and key in yearly_values["past"][year_str]:
                    yearly_values["all_years"][year_str][key] = yearly_values["past"][year_str][key]
                
                key = f"得意先_{suffix}"
                if year_str in yearly_values["current"] and key in yearly_values["current"][year_str]:
                    yearly_values["all_years"][year_str][key] = yearly_values["current"][year_str][key]
                elif year_str in yearly_values["past"] and key in yearly_values["past"][year_str]:
                    yearly_values["all_years"][year_str][key] = yearly_values["past"][year_str][key]
                
                key = f"得意先商品_{suffix}"
                if year_str in yearly_values["current"] and key in yearly_values["current"][year_str]:
                    yearly_values["all_years"][year_str][key] = yearly_values["current"][year_str][key]
                elif year_str in yearly_values["past"] and key in yearly_values["past"][year_str]:
                    yearly_values["all_years"][year_str][key] = yearly_values["past"][year_str][key]
        
        # 結果をログ出力
        print("結合前データの年度値:")
        for year_str, values in yearly_values["all_years"].items():
            print(f"  {year_str}年度:")
            for key, value in values.items():
                print(f"    {key}: {value:,.0f}")
        
        return yearly_values
    
    def get_yearly_values_after_merge(self, merged_data: Tuple) -> dict:
        """結合後データの各年度値を取得するメソッド"""
        print("\n=== 結合後データの年度値取得 ===")
        
        df1_merged, df2_merged, df3_merged = merged_data
        
        yearly_values = {}
        
        # 全年度の値を取得
        for year_str in self.years_list:
            yearly_values[year_str] = {}
            
            if year_str == self.current_year_str:
                suffixes = ["売上数", "㎡", "売上", "粗利"]
            else:
                suffixes = ["㎡", "売上", "粗利"]
            
            for suffix in suffixes:
                col_name = f"{year_str}{suffix}"
                
                # 法人データの合計値を取得
                if col_name in df1_merged.columns:
                    total_value = df1_merged.select(pl.col(col_name).sum()).item()
                    yearly_values[year_str][f"法人_{suffix}"] = total_value
                
                # 得意先データの合計値を取得
                if col_name in df2_merged.columns:
                    total_value2 = df2_merged.select(pl.col(col_name).sum()).item()
                    yearly_values[year_str][f"得意先_{suffix}"] = total_value2
                
                # 得意先商品データの合計値を取得
                if col_name in df3_merged.columns:
                    total_value3 = df3_merged.select(pl.col(col_name).sum()).item()
                    yearly_values[year_str][f"得意先商品_{suffix}"] = total_value3
        
        # 結果をログ出力
        print("結合後データの年度値:")
        for year_str, values in yearly_values.items():
            print(f"  {year_str}年度:")
            for key, value in values.items():
                print(f"    {key}: {value:,.0f}")
        
        return yearly_values
    
    def compare_yearly_values(self, before_values: dict, after_values: dict) -> List[dict]:
        """年度値比較検証ロジック"""
        print("\n=== 年度値比較検証 ===")
        
        differences = []
        
        # 全年度を比較
        for year_str in self.years_list:
            if year_str not in before_values["all_years"] or year_str not in after_values:
                continue
                
            before_year_data = before_values["all_years"][year_str]
            after_year_data = after_values[year_str]
            
            # 各データタイプと指標を比較
            for data_type in ["法人_", "得意先_", "得意先商品_"]:
                for suffix in ["売上数", "㎡", "売上", "粗利"]:
                    key = f"{data_type}{suffix}"
                    
                    # 売上数は当年度のみ比較
                    if suffix == "売上数" and year_str != self.current_year_str:
                        continue
                    
                    if key in before_year_data and key in after_year_data:
                        before_value = before_year_data[key]
                        after_value = after_year_data[key]
                        
                        # 数値比較（null値は0として扱う）
                        before_num = 0 if before_value is None else float(before_value)
                        after_num = 0 if after_value is None else float(after_value)
                        
                        # 差異が存在するかチェック
                        if before_num != after_num:
                            absolute_diff = abs(after_num - before_num)
                            relative_diff = 0.0
                            if before_num != 0:
                                relative_diff = absolute_diff / abs(before_num) * 100
                            
                            difference = {
                                "year": year_str,
                                "column": key,
                                "before_value": before_num,
                                "after_value": after_num,
                                "absolute_diff": absolute_diff,
                                "relative_diff": relative_diff
                            }
                            differences.append(difference)
                            
                            # 差異をログ出力
                            print(f"  差異検出: {year_str}年度 {key}")
                            print(f"    結合前: {before_num:,.0f}")
                            print(f"    結合後: {after_num:,.0f}")
                            print(f"    絶対差異: {absolute_diff:,.0f}")
                            print(f"    相対差異: {relative_diff:.2f}%")
        
        print(f"\n検査完了: {len(differences)}件の差異を検出")
        return differences
    
    def output_differences_warning(self, differences: List[dict]) -> None:
        """差異検出時の警告出力機能"""
        if not differences:
            print("\n=== 差異検証結果 ===")
            print("[OK] 差異は検出されませんでした")
            return
        
        print("\n" + "="*80)
        print("[WARNING] 差異検出警告")
        print("="*80)
        
        for i, diff in enumerate(differences, 1):
            print(f"\n差異 #{i}:")
            print(f"  発生年度: {diff['year']}年度")
            print(f"  発生列名: {diff['column']}")
            print(f"  結合前の値: {diff['before_value']:,.0f}")
            print(f"  結合後の値: {diff['after_value']:,.0f}")
            print(f"  差異の絶対値: {diff['absolute_diff']:,.0f}")
            print(f"  差異の相対値: {diff['relative_diff']:.2f}%")
            
            # 差異レベルの判定
            if diff['relative_diff'] > 10:
                level = "高"
                emoji = "[HIGH]"
            elif diff['relative_diff'] > 5:
                level = "中"
                emoji = "[MEDIUM]"
            else:
                level = "低"
                emoji = "[LOW]"
            
            print(f"  差異レベル: {emoji}{level}")
        
        print("\n" + "="*80)
        print(f"合計 {len(differences)}件の差異が検出されました")
        print("="*80)
    
    def _fix_full_join_nulls(self, df: pl.DataFrame) -> pl.DataFrame:
        """full join後のnull値を統一的に修復する（年度を意識した処理）"""
        
        
        # _past で終わる列を見つけて処理
        past_columns = [col for col in df.columns if col.endswith('_past')]
        
        # まず、IDカラム（法人コード、得意先コードなど）の処理
        for past_col in past_columns:
            base_col = past_col.replace('_past', '')
            
            # IDカラムの場合（数値データではない）
            if not any(year in base_col for year in self.years_list):
                if base_col not in df.columns:
                    df = df.with_columns(pl.col(past_col).alias(base_col))
                else:
                    # IDカラムはnullの場合のみ補完
                    df = df.with_columns([
                        pl.when(pl.col(base_col).is_null())
                        .then(pl.col(past_col))
                        .otherwise(pl.col(base_col))
                        .alias(base_col)
                    ])
        
        # 年度データの処理（当年度は当年度データから、過年度は_pastデータから取得）
        for year_str in self.years_list:
            # 売上数は当年度のみ処理
            if year_str == self.current_year_str:
                suffixes = ["売上数", "㎡", "売上", "粗利"]
            else:
                suffixes = ["㎡", "売上", "粗利"]
            
            for suffix in suffixes:
                col_name = f"{year_str}{suffix}"
                past_col_name = f"{col_name}_past"
                
                if year_str == self.current_year_str:
                    # 当年度データは既存のカラムをそのまま維持（修正なし）
                    # もしカラムが存在しない場合のみ作成
                    if col_name not in df.columns:
                        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col_name))
                    # 当年度データは_pastカラムで上書きしない（既存のデータを保持）
                else:
                    # 過年度データは_pastカラムから取得
                    if past_col_name in df.columns:
                        # 過年度カラムが存在しない場合、_pastから作成
                        # 過年度カラムが存在する場合、_pastの値で上書き
                        df = df.with_columns(pl.col(past_col_name).alias(col_name))
                    elif col_name not in df.columns:
                        # _pastカラムもcurrentカラムも存在しない場合、nullで作成
                        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col_name))
        
        # 全ての_past列を削除
        for past_col in past_columns:
            if past_col in df.columns:
                df = df.drop(past_col)
        
        return df
    
    def _add_past_suffix(self, df: pl.DataFrame, key_columns: List[str]) -> pl.DataFrame:
        """過年度データの列名に_pastサフィックスを追加（全列）"""
        
        # 全ての列に_pastサフィックスを追加（結合キーも含む）
        rename_mapping = {}
        for col in df.columns:
            rename_mapping[col] = f"{col}_past"
        
        if rename_mapping:
            df = df.rename(rename_mapping)
            
        return df
    
    def merge_data(self, current_data: Tuple, past_data: Tuple) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """当該年度と過年度データを結合（polars使用で高速化）"""
        print("\n=== データ結合処理（polars版） ===")
        
        df1_current, df2_current, df3_current = current_data
        df1_past, df2_past, df3_past = past_data
        
        # データ結合前の統計情報を表示
        print(f"当該年度法人データ: {len(df1_current):,}行")
        print(f"過年度法人データ: {len(df1_past):,}行")
        
        # 共通の法人コードを確認
        current_codes = set(df1_current["法人コード"].to_list())
        past_codes = set(df1_past["法人コード"].to_list())
        common_codes = current_codes & past_codes
        
        print(f"当該年度のみの法人コード: {len(current_codes - past_codes):,}件")
        print(f"過年度のみの法人コード: {len(past_codes - current_codes):,}件") 
        print(f"共通の法人コード: {len(common_codes):,}件")
        
        print("データ結合中...")
        
        # 過年度データの列名に_pastを事前に追加（全列）
        print("過年度データの列名調整中...")
        df1_past_renamed = self._add_past_suffix(df1_past, [])
        df2_past_renamed = self._add_past_suffix(df2_past, [])
        df3_past_renamed = self._add_past_suffix(df3_past, [])
        
        # 法人データの結合（外部結合で全ての実績を保持）
        df1_merged = df1_current.join(
            df1_past_renamed, how="full", 
            left_on=["法人コード"], right_on=["法人コード_past"]
        )
        
        # 得意先データの結合（外部結合で全ての実績を保持）
        df2_merged = df2_current.join(
            df2_past_renamed, how="full", 
            left_on=["得意先コード", "得意先名"], right_on=["得意先コード_past", "得意先名_past"]
        )
        
        # 得意先（商品）データの結合（外部結合で全ての実績を保持）
        df3_merged = df3_current.join(
            df3_past_renamed, 
            how="full",
            left_on=["営業所名", "得意先コード", "得意先名", "第1階層", "商品コード", "商品名"],
            right_on=["営業所名_past", "得意先コード_past", "得意先名_past", "第1階層_past", "商品コード_past", "商品名_past"]
        )

        # 結合後の統一的なnull値修復処理
        print("結合後のnull値修復処理中...")
        df1_merged = self._fix_full_join_nulls(df1_merged)
        df2_merged = self._fix_full_join_nulls(df2_merged)
        df3_merged = self._fix_full_join_nulls(df3_merged)
        print("全テーブルのnull値修復完了")
        
        print(f"法人データ結合完了: {len(df1_merged):,}行")
        print(f"得意先データ結合完了: {len(df2_merged):,}行")
        print(f"得意先（商品）データ結合完了: {len(df3_merged):,}行")
        return df1_merged, df2_merged, df3_merged
    
    def add_master_data(self, df1_merged: pl.DataFrame) -> pl.DataFrame:
        """法人マスタデータを結合（polars版）"""
        print("\n=== マスタデータ結合 ===")
        
        master_file = f"{self.master_filepath}/法人マスタ.csv"
        print(f"読み込み中: {master_file}")
        
        try:
            # polarsはcp932をサポートしていないため、pandasで読み込んでからpolarsに変換
            import pandas as pd
            df_master_pandas = pd.read_csv(master_file, encoding="cp932", usecols=["法人コード", "法人名"])
            df_master = pl.from_pandas(df_master_pandas)
            
            # polarsのjoinを使用（左結合）
            df1_merged = df1_merged.join(
                df_master, on=["法人コード"], how="left", suffix="_master"
            )
            
            # 法人名の欠損値補完（より堅牢な処理）
            if "法人名_master" in df1_merged.columns:
                df1_merged = df1_merged.with_columns(
                    pl.coalesce(pl.col("法人名"), pl.col("法人名_master")).alias("法人名")
                ).drop("法人名_master")
                
            # 念のためnullチェック
            null_count = df1_merged["法人名"].is_null().sum()
            if null_count > 0:
                print(f"警告: 法人名がnullの行が{null_count}件存在します")
            
            print("法人マスタ結合完了")
            return df1_merged
            
        except FileNotFoundError:
            print(f"マスタファイルが見つかりません: {master_file}")
            return df1_merged
    
    def process_product_data(self, df3_merged: pl.DataFrame) -> pl.DataFrame:
        """商品データの処理（polars版）"""
        print("\n=== 商品データ処理 ===")
        
        # 不要列削除（polarsのdropを使用）
        if "担当者名" in df3_merged.columns:
            df3_merged = df3_merged.drop("担当者名")
        
        # グループ化して集計（polarsの高速group_by）
        groupby_columns = ["営業所名", "得意先コード", "得意先名", "第1階層", "商品コード", "商品名"]
        numeric_columns = [col for col in df3_merged.columns if col not in groupby_columns]
        
        df3_merged = df3_merged.group_by(groupby_columns).agg([
            pl.col(col).sum() for col in numeric_columns
        ])
        
        print("商品データ処理完了")
        return df3_merged
    
    def reorder_columns(self, merged_data: Tuple) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """列順序の並び替え（polars版）"""
        print("\n=== 列順序調整 ===")
        
        df1_merged, df2_merged, df3_merged = merged_data
        
        # 年度別列を生成
        def generate_year_columns(df):
            generated_cols = []
            for year_str in self.years_list:
                # 売上数は当年度のみ含める
                if year_str == self.current_year_str:
                    suffixes = ["売上数", "㎡", "売上", "粗利"]
                else:
                    suffixes = ["㎡", "売上", "粗利"]
                
                for suffix in suffixes:
                    col_name = f"{year_str}{suffix}"
                    if col_name in df.columns:
                        generated_cols.append(col_name)
            return generated_cols
        
        # 法人データの列順序を定義
        columns_df1_ordered = ["法人コード", "法人名"] + generate_year_columns(df1_merged)
        existing_cols_df1 = [col for col in columns_df1_ordered if col in df1_merged.columns]
        df1_final = df1_merged.select(existing_cols_df1)

        # 得意先データの列順序を定義
        columns_df2_ordered = ["得意先コード", "得意先名"] + generate_year_columns(df2_merged)
        existing_cols_df2 = [col for col in columns_df2_ordered if col in df2_merged.columns]
        df2_final = df2_merged.select(existing_cols_df2)
        
        # 商品データの列順序を定義
        columns_df3_ordered = ["営業所名", "得意先コード", "得意先名", "第1階層", "商品コード", "商品名"] + generate_year_columns(df3_merged)
        existing_cols_df3 = [col for col in columns_df3_ordered if col in df3_merged.columns]
        df3_final = df3_merged.select(existing_cols_df3)

        print("列順序調整完了")
        return df1_final, df2_final, df3_final
    
    def sort_data(self, data: Tuple) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """データのソート（polars版）"""
        print("\n=== データソート ===")
        
        df1_final, df2_final, df3_final = data
        
        # 法人・得意先データのソート: 当年度売上列のみで降順
        # 当年度の売上列名を作成
        current_sales_column = f"{self.current_year_str}売上"
        
        if current_sales_column in df1_final.columns and current_sales_column in df2_final.columns:
            print(f"ソートキー: {current_sales_column} (降順, 当年度のみ)")
            
            # 当年度の売上のみでソート
            df1_final = df1_final.sort(current_sales_column, descending=True)
            df2_final = df2_final.sort(current_sales_column, descending=True)
            
            # デバッグ用: ソート後の売上値をログ出力（エンコード問題回避）
            print("\n法人データ ソート後売上値 (先頭5行):")
            sales_values = df1_final[current_sales_column][:5].to_list()
            print(f"{current_sales_column}: {sales_values}")
            
            print("\n得意先データ ソート後売上値 (先頭5行):")
            sales_values2 = df2_final[current_sales_column][:5].to_list()
            print(f"{current_sales_column}: {sales_values2}")
            
            print("法人・得意先データのソート完了")
        else:
            print("⚠ 売上列が存在しないため、ソートをスキップ")
        
        # 商品データのソート: 営業所名をカスタム順序でソート
        print("得意先（商品）データのカスタムソート中...")
        office_order = ["札幌", "仙台", "東京", "大阪", "名古屋", "福岡"]
        # カスタム順序マッピングの作成
        custom_order = {office: idx for idx, office in enumerate(office_order)}
        # マッピングにない営業所名は大きな値（末尾にソート）
        default_order = len(office_order)
        
        df3_final = df3_final.with_columns(
            pl.col("営業所名").map_elements(lambda x: custom_order.get(x, default_order)).alias("__office_order__")
        ).sort(
            ["__office_order__", "得意先名", "第1階層"],
            descending=[False, False, False]
        ).drop("__office_order__")
        
        print("得意先（商品）データのソート完了")
        
        # ソート結果をログ出力（エンコード問題回避）
        print("\n=== ソート結果サンプル ===")
        print("法人データ (先頭5行):")
        print(df1_final.head(5).to_pandas())
        print("\n得意先データ (先頭5行):")
        print(df2_final.head(5).to_pandas())
        
        return df1_final, df2_final, df3_final
    
    def fix_data_types(self, data: Tuple) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """データ型の修正（polars版）"""
        print("\n=== データ型変換 ===")
        
        df1_final, df2_final, df3_final = data
        
        print("データ型変換中...")
        
        # 法人コードを文字列に変換
        if "法人コード" in df1_final.columns:
            df1_final = df1_final.with_columns(
                pl.col("法人コード").cast(pl.Utf8).alias("法人コード")
            )
        
        # 得意先コードを文字列に変換
        if "得意先コード" in df2_final.columns:
            df2_final = df2_final.with_columns(
                pl.col("得意先コード").cast(pl.Utf8).alias("得意先コード")
            )
        
        if "得意先コード" in df3_final.columns:
            df3_final = df3_final.with_columns(
                pl.col("得意先コード").cast(pl.Utf8).alias("得意先コード")
            )
        
        # 商品コードを文字列に変換
        if "商品コード" in df3_final.columns:
            df3_final = df3_final.with_columns(
                pl.col("商品コード").cast(pl.Utf8).alias("商品コード")
            )
        
        # 年度別実績列のNull値を0に置き換え、売上列を浮動小数点数型に変換
        for df_name, df_final_ref in [("法人", df1_final), ("得意先", df2_final), ("商品", df3_final)]:
            target_cols = []
            for year_str in self.years_list:
                # 売上数は当年度のみ処理
                if year_str == self.current_year_str:
                    suffixes = ["売上数", "㎡", "売上", "粗利"]
                else:
                    suffixes = ["㎡", "売上", "粗利"]
                
                for suffix in suffixes:
                    col_name = f"{year_str}{suffix}"
                    if col_name in df_final_ref.columns:
                        target_cols.append(col_name)

            if target_cols:
                # 変換処理: 数値列を浮動小数点数型に変換し、nullを0で埋める
                new_df = df_final_ref.with_columns([
                    pl.col(col).fill_null(0).cast(pl.Float64)
                    if "売上" in col or "粗利" in col or "㎡" in col or "売上数" in col
                    else pl.col(col)
                    for col in target_cols
                ])
                
                # 更新するDataFrameを選択
                if df_name == "法人":
                    df1_final = new_df
                elif df_name == "得意先":
                    df2_final = new_df
                elif df_name == "商品":
                    df3_final = new_df

        print("データ型変換完了")
        return df1_final, df2_final, df3_final
    
    def filter_zero_rows(self, data: Tuple) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """年度別実績列がすべて0の行を除外する"""
        print("\n=== 年度別実績列がすべて0の行を除外 ===")
        
        df1_final, df2_final, df3_final = data
        names = ["法人", "得意先", "得意先（商品）"]
        
        # 年度別実績列の名前リストを生成
        year_columns = []
        for year in self.years_list:
            # 売上数は当年度のみフィルタリング対象に含める
            if year == self.current_year_str:
                suffixes = ["売上数", "㎡", "売上", "粗利"]
            else:
                suffixes = ["㎡", "売上", "粗利"]
            
            for suffix in suffixes:
                year_columns.append(f"{year}{suffix}")
        
        # 各データフレームに対してフィルタリング
        results = []
        for i, df in enumerate([df1_final, df2_final, df3_final]):
            # データフレームに存在する年度別列のみ抽出
            existing_cols = [col for col in year_columns if col in df.columns]
            
            if existing_cols:
                # フィルタリング前の行数を記録
                before_count = len(df)
                
                # 年度別列が全て0の行を除外
                df = df.filter(
                    pl.any_horizontal(pl.col(col) != 0 for col in existing_cols)
                )
                
                # フィルタリング結果をログ出力
                after_count = len(df)
                removed_count = before_count - after_count
                print(f"{names[i]}データ: {removed_count}行を除外 (全{after_count}行)")
            else:
                print(f"⚠ {names[i]}データ: フィルタリング対象の列が存在しないためスキップ")
            
            results.append(df)
        
        return tuple(results)

    def save_data(self, data: Tuple) -> None:
        """データファイルへの出力（polars版）"""
        print("\n=== データ出力 ===")
        
        # 出力ディレクトリ作成
        os.makedirs(self.output_dir, exist_ok=True)
        print("データファイルに出力中...")
        
        df1_final, df2_final, df3_final = data
        
        # 出力設定
        output_configs = [
            (df1_final, "法人_転記用データ.parquet", "法人"),
            (df2_final, "得意先_転記用データ.parquet", "得意先"),
            (df3_final, "得意先（商品）_転記用データ.parquet", "得意先（商品）")
        ]
        
        def save_single_file(config):
            df, filename, name = config
            filepath = f"{self.output_dir}/{filename}"
            # polarsの高速parquet書き込み（圧縮付き）
            df.write_parquet(filepath, compression="snappy")
            print(f"{name}データを {filepath} に保存しました ({len(df):,}行)")
            return True
        
        # 並列保存で高速化
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(save_single_file, config) for config in output_configs]
            for future in as_completed(futures):
                future.result()  # エラーチェックのために結果を取得
    
    def run(self) -> None:
        """メイン処理実行（polars最適化版 + 検証機能追加）"""
        start_time = time.time()
        print("=" * 50)
        print("=== データ結合処理開始（polars最適化版 + 検証機能） ===")
        print("=" * 50)
        print(f"処理対象年度: {self.current_year_str}")
        print(f"比較対象年度: {self.last_year_str}")
        print(f"年度リスト: {', '.join(self.years_list)}")
        print("最適化ポイント: polars + 並列処理 + 圧縮 + データ検証")
        
        try:
            # 1. データ読み込み（並列化済み）
            print("\n" + "="*60)
            print("ステップ1: 当該年度データ読み込み")
            print("="*60)
            current_data = self.load_current_year_data()
            
            # 【検証タイミング1】当該年度データ読み込み後の検証
            print("\n" + "="*60)
            print("検証タイミング1: 当該年度データ読み込み後")
            print("="*60)
            # 当該年度データのみの検証（過年度データがないため簡易検証）
            current_yearly_values = self.get_yearly_values_before_merge(current_data, (None, None, None))
            print("[OK] 当該年度データの整合性検証完了")
            
            # 2. 過年度データ読み込み
            print("\n" + "="*60)
            print("ステップ2: 過年度データ読み込み")
            print("="*60)
            past_data = self.load_past_year_data()
            
            # 【検証タイミング2】過年度データ読み込み後の検証
            print("\n" + "="*60)
            print("検証タイミング2: 過年度データ読み込み後")
            print("="*60)
            # 過年度データのみの検証
            past_yearly_values = self.get_yearly_values_before_merge((None, None, None), past_data)
            print("[OK] 過年度データの整合性検証完了")
            
            # 3. 過年度データの前処理
            print("\n" + "="*60)
            print("ステップ3: 過年度データ前処理")
            print("="*60)
            _, df2_past, df3_past = past_data
            df2_past, df3_past = self.preprocess_past_data(df2_past, df3_past)
            past_data = (past_data[0], df2_past, df3_past)
            
            # 4. データ結合（polarsの高速join）
            print("\n" + "="*60)
            print("ステップ4: データ結合処理")
            print("="*60)
            merged_data = self.merge_data(current_data, past_data)
            
            # 【検証タイミング3】データ結合後の検証
            print("\n" + "="*60)
            print("検証タイミング3: データ結合後")
            print("="*60)
            # 結合前データの年度値を取得
            before_merge_values = self.get_yearly_values_before_merge(current_data, past_data)
            
            # 結合後データの年度値を取得
            after_merge_values = self.get_yearly_values_after_merge(merged_data)
            
            # 年度値比較検証
            differences = self.compare_yearly_values(before_merge_values, after_merge_values)
            
            # 差異検出時の警告出力
            self.output_differences_warning(differences)
            
            # 5. マスタデータ結合
            print("\n" + "="*60)
            print("ステップ5: マスタデータ結合")
            print("="*60)
            df1_merged = self.add_master_data(merged_data[0])
            merged_data = (df1_merged, merged_data[1], merged_data[2])
            
            # 6. 商品データ処理
            print("\n" + "="*60)
            print("ステップ6: 商品データ処理")
            print("="*60)
            df3_processed = self.process_product_data(merged_data[2])
            merged_data = (merged_data[0], merged_data[1], df3_processed)
            
            # 7. 列順序調整
            print("\n" + "="*60)
            print("ステップ7: 列順序調整")
            print("="*60)
            reordered_data = self.reorder_columns(merged_data)
            
            # 8. データ型修正（ソート前にnull値を0に変換）
            print("\n" + "="*60)
            print("ステップ8: データ型修正")
            print("="*60)
            typed_data = self.fix_data_types(reordered_data)
            
            # 9. ソート処理（polarsの高速sort）
            print("\n" + "="*60)
            print("ステップ9: データソート")
            print("="*60)
            final_data = self.sort_data(typed_data)
            
            # 10. 年度別実績列がすべて0の行を除外
            print("\n" + "="*60)
            print("ステップ10: 0行除外処理")
            print("="*60)
            final_data = self.filter_zero_rows(final_data)
            
            # 【検証タイミング4】最終的なデータ保存前の検証
            print("\n" + "="*60)
            print("検証タイミング4: 最終データ保存前")
            print("="*60)
            # 最終データの年度値を取得
            final_yearly_values = self.get_yearly_values_after_merge(final_data)
            
            # 結合後データとの比較検証（形式を変換）
            after_merge_values_for_comparison = {
                "all_years": after_merge_values
            }
            final_differences = self.compare_yearly_values(after_merge_values_for_comparison, final_yearly_values)
            self.output_differences_warning(final_differences)
            
            # 11. データ保存（並列保存）
            print("\n" + "="*60)
            print("ステップ11: データ保存")
            print("="*60)
            self.save_data(final_data)
            
            # 処理完了
            end_time = time.time()
            elapsed_time = end_time - start_time
            print("\n" + "=" * 60)
            print("全ての処理が完了しました（polars最適化版 + 検証機能）")
            print(f"処理時間: {elapsed_time:.2f}秒")
            print("使用ライブラリ: polars (高速データ処理)")
            print("最適化機能: 並列I/O + 高速join + 圧縮parquet")
            print("追加機能: データ結合前後の値比較検証")
            print("=" * 60)
            
        except Exception as e:
            print(f"\nエラーが発生しました: {e}")
            raise

def main():
    """メイン処理"""
    processor = DataProcessor()
    processor.run()

if __name__ == "__main__":
    main()
