@echo off
REM マッピング名パラメータのチェック
if "%1"=="" (
    echo エラー: マッピング名を指定してください
    echo 使用方法: transfer.bat <マッピング名>
    exit /b 1
)

REM Pythonスクリプトの実行
python main3.py %1

REM 実行結果のチェック
if errorlevel 1 (
    echo エラー: 移行処理が失敗しました
    exit /b 1
) else (
    echo 移行処理が正常に完了しました
    exit /b 0
) 