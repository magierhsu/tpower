export function calculateSummary(data, fuelTypes) {
    let totalGeneration = 0;
    let renewableGeneration = 0;
    const fuelTypeTotals = {};

    fuelTypes.forEach(type => {
        fuelTypeTotals[type] = 0;
    });

    data.forEach(row => {
        let rowTotal = 0;
        fuelTypes.forEach(type => {
            const value = row[type] || 0; // 確保有值，沒有則為0
            rowTotal += value;
            fuelTypeTotals[type] += value;

            // 判斷是否為再生能源
            if (['水力', '風力', '太陽能', '其它再生'].includes(type)) {
                renewableGeneration += value;
            }
        });
        totalGeneration += rowTotal;
    });

    const renewablePercentage = totalGeneration > 0 ? (renewableGeneration / totalGeneration * 100).toFixed(2) : 0;

    return {
        totalGeneration: totalGeneration.toFixed(2),
        renewablePercentage: renewablePercentage,
        nuclearGeneration: fuelTypeTotals['核能'] ? fuelTypeTotals['核能'].toFixed(2) : '0.00',
        coalGeneration: fuelTypeTotals['燃煤'] ? fuelTypeTotals['燃煤'].toFixed(2) : '0.00',
        gasGeneration: fuelTypeTotals['燃氣'] ? fuelTypeTotals['燃氣'].toFixed(2) : '0.00',
        fuelTypeTotals: fuelTypeTotals // 包含所有燃料類型的總計
    };
}

export function renderSummary(summaryData) {
    document.getElementById('totalGeneration').textContent = summaryData.totalGeneration + ' MW';
    document.getElementById('renewablePercentage').textContent = summaryData.renewablePercentage + '%';
    document.getElementById('nuclearGeneration').textContent = summaryData.nuclearGeneration + ' MW';
    document.getElementById('coalGeneration').textContent = summaryData.coalGeneration + ' MW';
    document.getElementById('gasGeneration').textContent = summaryData.gasGeneration + ' MW';
}

export function calculateTimePointStatistics(dataPoint) {
    const aggregatedData = new Map(); // 用於按機組類型匯總數據
    const allDetails = []; // 用於儲存所有詳細機組數據，包括「小計」

    // 輔助函數：從字串中提取數值 (例如 "951.0(1.654%)" -> 951.0)
    const extractNumericValue = (str) => {
        if (typeof str !== 'string') return parseFloat(str) || 0;
        const match = str.match(/^(\d+(\.\d+)?)/);
        return match ? parseFloat(match[1]) : 0;
    };

    // 遍歷 aaData 陣列中的每個機組數據
    dataPoint.aaData.forEach(item => {
        const type = item['機組類型'];
        const name = item['機組名稱'];
        const capacity = extractNumericValue(item['裝置容量(MW)']);
        const netGeneration = extractNumericValue(item['淨發電量(MW)']);
        const generationRatio = extractNumericValue(item['淨發電量/裝置容量比(%)']);
        const remark = item['備註'];

        // 將所有詳細數據加入 allDetails，包括「小計」行
        allDetails.push({
            '機組類型': type,
            '機組名稱': name,
            '裝置容量(MW)': capacity,
            '淨發電量(MW)': netGeneration,
            '淨發電量/裝置容量比(%)': generationRatio,
            '備註': remark
        });

        // 排除「小計」行，因為我們將自行計算總和
        if (name === '小計') {
            return;
        }

        if (!aggregatedData.has(type)) {
            aggregatedData.set(type, {
                '機組類型': type,
                '裝置容量(MW)': 0,
                '淨發電量(MW)': 0,
                '淨發電量/裝置容量比(%)': 0, // 這個欄位在匯總時需要重新計算
                '備註': '', // 匯總時備註可能不適用
                'details': [] // 儲存該類型下的詳細機組數據
            });
        }

        const currentTypeData = aggregatedData.get(type);
        currentTypeData['裝置容量(MW)'] += capacity;
        currentTypeData['淨發電量(MW)'] += netGeneration;
        currentTypeData['details'].push({
            '機組類型': type,
            '機組名稱': name,
            '裝置容量(MW)': capacity,
            '淨發電量(MW)': netGeneration,
            '淨發電量/裝置容量比(%)': generationRatio,
            '備註': remark
        });
    });

    const statistics = Array.from(aggregatedData.values());

    // 計算每個機組類型的「淨發電量/裝置容量比(%)」
    statistics.forEach(item => {
        if (item['裝置容量(MW)'] > 0) {
            item['淨發電量/裝置容量比(%)'] = (item['淨發電量(MW)'] / item['裝置容量(MW)'] * 100).toFixed(2);
        } else {
            item['淨發電量/裝置容量比(%)'] = '0.00';
        }
        item['裝置容量(MW)'] = item['裝置容量(MW)'].toFixed(2);
        item['淨發電量(MW)'] = item['淨發電量(MW)'].toFixed(2);
    });

    return statistics; // 返回匯總數據，每個匯總數據包含其詳細數據
}

export function renderTimePointTable(statistics) {
    const tableBody = document.querySelector('#timePointTable tbody');
    tableBody.innerHTML = ''; // 清空現有內容

    if (statistics.length === 0) {
        const row = tableBody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = 6; // 跨越所有列
        cell.textContent = '無資料可顯示。';
        cell.style.textAlign = 'center';
        return;
    }

    statistics.forEach(item => {
        // 渲染總計行
        const totalRow = tableBody.insertRow();
        totalRow.classList.add('total-row'); // 添加樣式類別
        totalRow.insertCell().textContent = item['機組類型'];
        totalRow.insertCell().textContent = '總計'; // 機組名稱顯示「總計」
        totalRow.insertCell().textContent = item['裝置容量(MW)'] + ' MW';
        totalRow.insertCell().textContent = item['淨發電量(MW)'] + ' MW';
        totalRow.insertCell().textContent = item['淨發電量/裝置容量比(%)'] + '%';
        totalRow.insertCell().textContent = item['備註']; // 總計行的備註可以留空或顯示特定內容

        // 渲染詳細機組行
        item.details.forEach(detail => {
            const detailRow = tableBody.insertRow();
            detailRow.classList.add('detail-row'); // 添加樣式類別
            detailRow.insertCell().textContent = ''; // 機組類型留空或縮進
            detailRow.insertCell().textContent = detail['機組名稱'];
            detailRow.insertCell().textContent = (detail['裝置容量(MW)'] === 0 ? '--' : detail['裝置容量(MW)'].toFixed(2)) + ' MW';
            detailRow.insertCell().textContent = detail['淨發電量(MW)'].toFixed(2) + ' MW';
            detailRow.insertCell().textContent = detail['淨發電量/裝置容量比(%)'].toFixed(2) + '%';
            detailRow.insertCell().textContent = detail['備註'];
        });
    });
}