import * as dataLoader from './dataLoader.js';
import * as dateFilter from './dateFilter.js';
import * as dataSummary from './dataSummary.js';
import * as chartRenderer from './chartRenderer.js';

let allData = []; // 儲存所有載入的資料 (CSV + JSON)
let timeSeriesChartInstance;
let sourceDistributionChartInstance;

// 發電來源的順序和顏色定義
const fuelTypes = [
    '核能', '燃煤', '汽電共生', '民營燃煤', '燃氣', '民營燃氣', '燃油', '輕油',
    '水力', '風力', '太陽能', '其它再生', '儲能', '儲能負載'
];

const fuelColors = {
    '核能': '#FFD700', // Gold
    '燃煤': '#8B4513', // SaddleBrown
    '汽電共生': '#A9A9A9', // DarkGray
    '民營燃煤': '#696969', // DimGray
    '燃氣': '#ADD8E6', // LightBlue
    '民營燃氣': '#87CEEB', // SkyBlue
    '燃油': '#4682B4', // SteelBlue
    '輕油': '#5F9EA0', // CadetBlue
    '水力': '#4169E1', // RoyalBlue
    '風力': '#3CB371', // MediumSeaGreen
    '太陽能': '#FF8C00', // DarkOrange
    '其它再生': '#20B2AA', // LightSeaGreen
    '儲能': '#9370DB', // MediumPurple
    '儲能負載': '#DA70D6' // Orchid
};


async function initializeDashboard() {
    try {
        // 載入 CSV 資料
        const csvData = await dataLoader.loadCSV('db.csv');
        allData = csvData; // 初始只用 CSV 資料

        // 初始化日期選擇器和時間點選擇器
        dateFilter.initDatePickers();
        dateFilter.initTimePointSelector();

        // 設置預設日期範圍為最近一天的數據
        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(today.getDate() - 1);

        const defaultStartDate = yesterday.toISOString().split('T')[0];
        const defaultEndDate = today.toISOString().split('T')[0];

        document.getElementById('startDate').value = defaultStartDate;
        document.getElementById('endDate').value = defaultEndDate;

        // 根據預設日期範圍篩選資料並更新儀表板
        await updateDashboard();

        // 綁定事件監聽器
        document.getElementById('applyFilter').addEventListener('click', updateDashboard);
        document.getElementById('applyTimePointFilter').addEventListener('click', handleTimePointFilter);

    } catch (error) {
        console.error('儀表板初始化失敗:', error);
        alert('儀表板初始化失敗，請檢查控制台。');
    }
}

async function updateDashboard() {
    const { startDate, endDate } = dateFilter.getFilterDates();
    const filteredData = dateFilter.filterDataByDateRange(allData, startDate, endDate);

    // 計算並顯示資料摘要
    const summary = dataSummary.calculateSummary(filteredData, fuelTypes);
    dataSummary.renderSummary(summary);

    // 繪製時間序列圖
    const timeSeriesChartData = chartRenderer.prepareTimeSeriesChartData(filteredData, fuelTypes, fuelColors);
    timeSeriesChartInstance = chartRenderer.renderTimeSeriesChart(timeSeriesChartData, 'timeSeriesChart', timeSeriesChartInstance);

    // 輸出時間序列數據為表格
    dataSummary.renderTimeSeriesTable(filteredData, fuelTypes);

    // 繪製發電來源佔比圓餅圖
    const sourceDistributionChartData = chartRenderer.prepareSourceDistributionChartData(summary, fuelColors);
    sourceDistributionChartInstance = chartRenderer.renderPieChart(sourceDistributionChartData, 'sourceDistributionChart', sourceDistributionChartInstance);
}

async function handleTimePointFilter() {
    const selectedTimePoint = dateFilter.getSelectTimePoint();
    if (!selectedTimePoint) {
        alert('請選擇一個時間點。');
        return;
    }

    const selectedDate = new Date(selectedTimePoint);
    const yearMonth = selectedDate.getFullYear() + (selectedDate.getMonth() + 1).toString().padStart(2, '0');
    const jsonFileName = `db_files/db_${yearMonth}.json`;

    try {
        const jsonData = await dataLoader.loadJSON(jsonFileName);
        if (!jsonData || jsonData.length === 0) {
            alert(`找不到 ${yearMonth} 的即時資料或資料為空。`);
            document.getElementById('time-point-table-display').style.display = 'none';
            return;
        }

        // 尋找最接近選定時間點的數據
        // 確保 selectedDate 是基於本地時間的，並格式化為 YYYY-MM-DDTHH:MM:SS
        const year = selectedDate.getFullYear();
        const month = (selectedDate.getMonth() + 1).toString().padStart(2, '0');
        const day = selectedDate.getDate().toString().padStart(2, '0');
        const hours = selectedDate.getHours().toString().padStart(2, '0');
        const minutes = selectedDate.getMinutes().toString().padStart(2, '0');
        const seconds = selectedDate.getSeconds().toString().padStart(2, '0'); // JSON 數據包含秒數

        const targetTimestamp = `${year}-${month}-${day}T${hours}:${minutes}:${seconds}`;

        const dataPoint = jsonData.find(d => d.DateTime === targetTimestamp);

        if (dataPoint && dataPoint.aaData && dataPoint.aaData.length > 0) {
            const result = dataSummary.calculateTimePointStatistics(dataPoint);
            dataSummary.renderTimePointTable(result);
            document.getElementById('timePointDisplay').textContent = `(${selectedTimePoint})`; // 顯示選定的時間點
            document.getElementById('time-point-table-display').style.display = 'block';
        } else {
            alert(`在 ${yearMonth} 的資料中找不到 ${selectedTimePoint} 的數據點或該時間點無詳細機組資料。`);
            document.getElementById('time-point-table-display').style.display = 'none';
        }

    } catch (error) {
        console.error('載入或處理即時資料失敗:', error);
        alert(`載入或處理 ${yearMonth} 的即時資料失敗，請檢查控制台。`);
        document.getElementById('time-point-table-display').style.display = 'none';
    }
}

// 頁面載入完成後初始化儀表板
document.addEventListener('DOMContentLoaded', initializeDashboard);