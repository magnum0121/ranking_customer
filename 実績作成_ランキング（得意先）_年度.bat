@echo off
REM 年度ランキング（得意先）データ作成バッチファイル

REM エラーハンドリングを有効化
setlocal enabledelayedexpansion

REM Pythonスクリプトの実行
python 01_データ作成.py
if %errorlevel% neq 0 (
    echo エラー: 01_データ作成.pyの実行に失敗しました
    pause
    exit /b 1
)

python 02_過去データ結合.py
if %errorlevel% neq 0 (
    echo エラー: 02_過去データ結合.pyの実行に失敗しました
    pause
    exit /b 1
)

python 03_データ転記.py
if %errorlevel% neq 0 (
    echo エラー: 03_データ転記.pyの実行に失敗しました
    pause
    exit /b 1
)

echo 全ての処理が正常に完了しました
exit