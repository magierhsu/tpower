# 台灣電力公司發電量數據收集專案

這個專案旨在自動收集台灣電力公司的發電量數據，並將其儲存為 CSV 和 JSON 格式的資料庫。

## 專案檔案說明

以下是專案中每個主要檔案的功能、用途、相關數據來源 URL 以及它們如何協同工作：

### [`README.md`](README.md)

*   **功能**: 本檔案提供專案的總體概述、檔案說明以及各組件如何協同工作的資訊。
*   **用途**: 作為專案的入門指南和參考文件。

### [`collect_data.py`](collect_data.py)

*   **功能**: 這個 Python 腳本負責從台灣電力公司的網站下載每日發電量數據（CSV 格式）。它會獲取今天和昨天的數據，並將其追加到 `db.csv` 檔案中。
*   **用途**: 建立並維護一個包含歷史發電量數據的 CSV 檔案。它每 5 分鐘運行一次，以確保數據是最新的。在凌晨 00:00 到 00:30 之間會暫停數據收集，以避免在數據更新期間的潛在問題。
*   **數據來源 URL**:
    *   `https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadfueltype.csv`
    *   `https://www.taipower.com.tw/d006/loadGraph/loadGraph/data/loadfueltype_1.csv`

### [`update_db.py`](update_db.py)

*   **功能**: 這個 Python 腳本負責從台灣電力公司的另一個 API 下載即時發電量數據（JSON 格式）。它會將新數據追加到按月份組織的 JSON 檔案中（例如：`db_files/db_202506.json`）。
*   **用途**: 建立並維護一個包含即時發電量數據的 JSON 檔案集合。它每 1 分鐘運行一次，以確保 JSON 數據庫是最新的。數據會根據時間戳儲存到對應月份的檔案中。
*   **數據來源 URL**:
    *   `https://service.taipower.com.tw/data/opendata/apply/file/d006001/001.json`

## 協同工作方式

[`collect_data.py`](collect_data.py) 和 [`update_db.py`](update_db.py) 這兩個 Python 腳本獨立運行，但都持續從台灣電力公司獲取數據。[`collect_data.py`](collect_data.py) 負責收集每日的歷史數據，而 [`update_db.py`](update_db.py) 則負責收集更頻繁的即時數據。
