# 本地網頁結構和所需組件規劃

## 1. HTML 結構 (`index.html`)

`index.html` 將作為網頁的入口點，包含以下主要區塊：

```html
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>發電量分析儀表板</title>
    <link rel="stylesheet" href="css/style.css">
</head>
<body>
    <header>
        <h1>台灣發電量分析儀表板</h1>
    </header>

    <main>
        <section id="controls">
            <h2>資料篩選</h2>
            <div class="date-picker-container">
                <label for="startDate">開始日期:</label>
                <input type="date" id="startDate">
                <label for="endDate">結束日期:</label>
                <input type="date" id="endDate">
                <button id="applyFilter">應用篩選</button>
            </div>
            <div class="data-source-toggle">
                <!-- 如果需要切換資料來源，可以在這裡添加選項 -->
                <!-- <label><input type="checkbox" id="showJsonData"> 顯示即時資料</label> -->
            </div>
            <div class="time-point-selector">
                <h3>選擇時間點 (每10分鐘)</h3>
                <input type="datetime-local" id="timePointSelector">
                <button id="applyTimePointFilter">顯示該時段機組統計</button>
            </div>
        </section>

        <section id="summary-display">
            <h2>資料摘要</h2>
            <div class="summary-cards">
                <div class="card">
                    <h3>總發電量 (MW)</h3>
                    <p id="totalGeneration">--</p>
                </div>
                <div class="card">
                    <h3>再生能源佔比</h3>
                    <p id="renewablePercentage">--</p>
                </div>
                <div class="card">
                    <h3>核能發電量 (MW)</h3>
                    <p id="nuclearGeneration">--</p>
                </div>
                <div class="card">
                    <h3>燃煤發電量 (MW)</h3>
                    <p id="coalGeneration">--</p>
                </div>
                <div class="card">
                    <h3>燃氣發電量 (MW)</h3>
                    <p id="gasGeneration">--</p>
                </div>
                <!-- 可以根據需要增加更多發電來源的摘要 -->
            </div>
        </section>

        <section id="time-point-table-display" style="display:none;">
            <h2>單一時間點機組發電統計</h2>
            <table id="timePointTable">
                <thead>
                    <tr>
                        <th>機組類型</th>
                        <th>發電量 (MW)</th>
                        <th>佔比 (%)</th>
                    </tr>
                </thead>
                <tbody>
                    <!-- 資料將由 JavaScript 動態載入 -->
                </tbody>
            </table>
        </section>

        <section id="chart-display">
            <h2>發電量趨勢</h2>
            <div class="chart-container">
                <canvas id="timeSeriesChart"></canvas>
            </div>
            <h2>發電來源佔比</h2>
            <div class="chart-container pie-chart-container">
                <canvas id="sourceDistributionChart"></canvas>
            </div>
        </section>
    </main>

    <footer>
        <p>&copy; 2025 台灣發電量分析儀表板</p>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script type="module" src="js/main.js"></script>
</body>
</html>
```

## 2. CSS 需求 (`css/style.css`)

為了美化頁面和佈局，需要以下基本的 CSS 樣式：

*   **通用樣式**：`body`, `html` 的字體、顏色、邊距重置。
*   **佈局**：使用 Flexbox 或 CSS Grid 實現整體頁面佈局（header, main, footer）以及各區塊內部的元件排列。
*   **響應式設計**：使用 `@media` 查詢確保網頁在不同螢幕尺寸下（桌面、平板、手機）都能良好顯示。
*   **元件樣式**：
    *   `header` 和 `footer` 的背景色、文字顏色、內邊距。
    *   `section` 區塊的間距、背景色、陰影等。
    *   `input[type="date"]`, `input[type="datetime-local"]` 和 `button` 的樣式，使其更具現代感。
    *   `summary-cards` 和 `card` 的樣式，例如卡片邊框、內邊距、文字對齊。
    *   `chart-container` 的尺寸和邊距。
    *   `time-point-table` 的樣式，包括表格邊框、表頭、行間距等。
*   **交互效果**：例如按鈕的 `:hover` 狀態。

## 3. JavaScript 模組/功能 (`js/` 目錄下)

JavaScript 將負責資料處理、邏輯控制和視覺化。建議將功能拆分為多個模組以提高可維護性。

*   **`js/dataLoader.js`**：
    *   **功能**：負責從指定路徑載入並解析 CSV 和 JSON 資料。
    *   **方法**：
        *   `async loadCSV(path)`: 使用 `fetch` 載入 CSV 檔案，並解析為 JavaScript 物件陣列。
        *   `async loadJSON(path)`: 使用 `fetch` 載入 JSON 檔案，並解析為 JavaScript 物件。
        *   `async loadDataForMonth(yearMonth)`: 根據 `YYYYMM` 格式的月份載入對應的 JSON 檔案 (`db_files/db_YYYYMM.json`)。
        *   `normalizeData(csvData, jsonData)`: 將兩種來源的資料合併成統一的格式，以便後續處理。所有發電量數據都以數值類型儲存，時間戳記統一為 `Date` 物件。
        *   **效能考量**：對於大型 `db_files`，考慮只載入所需月份的 JSON 檔案，而不是一次性載入所有歷史數據。

*   **`js/dateFilter.js`**：
    *   **功能**：處理日期選擇器的初始化和資料篩選邏輯。
    *   **方法**：
        *   `initDatePickers()`: 獲取 HTML 中的日期輸入框元素，並設置預設值（例如，當前月份的開始和結束）。
        *   `getFilterDates()`: 獲取當前日期選擇器中的開始和結束日期。
        *   `filterDataByDateRange(data, startDate, endDate)`: 根據提供的日期範圍篩選資料陣列。
        *   `initTimePointSelector()`: 初始化時間點選擇器，設置預設值。
        *   `getSelectTimePoint()`: 獲取選定的時間點。
        *   **事件處理**：監聽日期輸入框的 `change` 事件或「應用篩選」按鈕的 `click` 事件，以及時間點選擇器和其應用按鈕的事件，觸發資料重新篩選和頁面更新。

*   **`js/dataSummary.js`**：
    *   **功能**：計算篩選後資料的摘要指標。
    *   **方法**：
        *   `calculateSummary(filteredData)`: 接收篩選後的資料，計算總發電量、各發電來源的總和、再生能源（風力、太陽能、水力、其它再生）佔比等。
        *   `renderSummary(summaryData)`: 將計算出的摘要資料更新到 `summary-display` 區塊的 HTML 元素中。
        *   `calculateTimePointStatistics(dataPoint)`: 計算單一時間點各機組的發電量和佔比。
        *   `renderTimePointTable(statistics)`: 將單一時間點的統計資料渲染到 `time-point-table` 中。

*   **`js/chartRenderer.js`**：
    *   **功能**：負責資料的視覺化，繪製圖表。
    *   **建議圖表庫**：**Chart.js**。它輕量、易於使用，且功能強大，適合快速實現多種常見圖表類型。
    *   **方法**：
        *   `renderTimeSeriesChart(data, elementId, labels, datasets)`: 繪製多條折線圖，顯示不同發電來源的發電量隨時間的變化。
        *   `renderPieChart(data, elementId, labels, values)`: 繪製圓餅圖，顯示各發電來源在總發電量中的佔比。
        *   `updateChart(chartInstance, newData)`: 更新現有圖表的資料。

*   **`js/main.js`** (或 `js/app.js`)：
    *   **功能**：作為應用程式的主入口，協調所有模組的運作。
    *   **初始化流程**：
        1.  頁面載入完成後，呼叫 `dataLoader.loadCSV('db.csv')` 載入 CSV 資料。
        2.  初始化 `dateFilter` 的日期選擇器和時間點選擇器。
        3.  根據預設日期範圍篩選 CSV 資料。
        4.  呼叫 `dataSummary` 計算並顯示摘要。
        5.  呼叫 `chartRenderer` 繪製初始時間序列圖和圓餅圖。
    *   **事件處理**：
        *   監聽「應用篩選」按鈕的 `click` 事件：
            *   獲取日期範圍，呼叫 `dateFilter` 篩選 CSV 資料。
            *   呼叫 `dataSummary` 和 `chartRenderer` 更新頁面顯示。
        *   監聽「顯示該時段機組統計」按鈕的 `click` 事件：
            *   獲取選定的時間點。
            *   根據時間點判斷所需 JSON 檔案的月份，呼叫 `dataLoader.loadDataForMonth()` 載入對應的 JSON 資料。
            *   從載入的 JSON 資料中找到該時間點的數據。
            *   呼叫 `dataSummary.calculateTimePointStatistics()` 計算統計。
            *   呼叫 `dataSummary.renderTimePointTable()` 顯示表格。

## 4. 檔案結構建議

為了保持專案的整潔和可擴展性，建議採用以下檔案結構：

```
/home/yuwei1_hsu/hdd/tpower/
├── index.html                  # 網頁主入口文件
├── db.csv                      # 原始 CSV 資料文件
├── db_files/                   # 存放 JSON 資料的目錄
│   └── db_YYYYMM.json          # 原始 JSON 資料文件 (例如 db_202506.json)
├── css/                        # 存放 CSS 樣式文件的目錄
│   └── style.css               # 主要的 CSS 樣式表
└── js/                         # 存放 JavaScript 模組的目錄
    ├── main.js                 # 應用程式主邏輯，協調各模組
    ├── dataLoader.js           # 資料載入和解析模組
    ├── dateFilter.js           # 日期篩選邏輯模組
    ├── dataSummary.js          # 資料摘要計算模組
    └── chartRenderer.js        # 圖表繪製模組
```

## 規劃流程圖 (Mermaid)

```mermaid
graph TD
    A[使用者開啟 index.html] --> B{載入 HTML, CSS, JS};
    B --> C[main.js 啟動];
    C --> D[dataLoader.js: 載入 db.csv];
    D --> E[dateFilter.js: 初始化日期選擇器 & 時間點選擇器];
    E --> F[dateFilter.js: 獲取預設日期範圍];
    F --> G[dateFilter.js: 篩選 CSV 資料];
    G --> H[dataSummary.js: 計算資料摘要];
    H --> I[dataSummary.js: 渲染摘要到 HTML];
    H --> J[chartRenderer.js: 繪製時間序列圖];
    H --> K[chartRenderer.js: 繪製發電來源佔比圓餅圖];
    K --> L[顯示初始儀表板];

    subgraph 日期範圍篩選
        M[日期選擇器變更 / 應用篩選按鈕點擊] --> F;
    end

    subgraph 單一時間點統計
        N[時間點選擇器變更 / 顯示按鈕點擊] --> O[dateFilter.js: 獲取選定時間點];
        O --> P{判斷所需 JSON 檔案月份};
        P --> Q[dataLoader.js: 載入 db_files/db_YYYYMM.json];
        Q --> R[dataSummary.js: 計算單一時間點統計];
        R --> S[dataSummary.js: 渲染統計表格];
        S --> T[顯示單一時間點統計表格];
    end