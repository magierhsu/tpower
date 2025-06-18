export async function loadCSV(path) {
    try {
        const response = await fetch(path);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const text = await response.text();
        return parseCSV(text);
    } catch (error) {
        console.error(`載入或解析 CSV 失敗: ${path}`, error);
        return [];
    }
}

export async function loadJSON(path) {
    try {
        const response = await fetch(path);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const json = await response.json();
        return json;
    } catch (error) {
        console.error(`載入或解析 JSON 失敗: ${path}`, error);
        return [];
    }
}

// 簡單的 CSV 解析器，假設第一行為標頭
function parseCSV(text) {
    const lines = text.trim().split('\n');
    if (lines.length === 0) return [];

    const headers = lines[0].split(',').map(h => h.trim());
    const data = [];

    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map(v => v.trim());
        if (values.length !== headers.length) {
            console.warn(`CSV 行數不匹配標頭數: ${lines[i]}`);
            continue;
        }
        const row = {};
        headers.forEach((header, index) => {
            // 嘗試將數值轉換為數字，否則保留為字串
            row[header] = isNaN(Number(values[index])) ? values[index] : Number(values[index]);
        });
        data.push(row);
    }
    return data;
}

// 由於 CSV 和 JSON 數據結構可能不同，這裡提供一個範例的 normalizeData 函數
// 實際應用中需要根據兩種數據的具體格式來實現
export function normalizeData(csvData, jsonData) {
    // 這裡假設我們主要使用 CSV 數據，並將 JSON 數據作為補充或即時數據
    // 如果 JSON 數據包含 CSV 中沒有的欄位，可以在這裡合併
    // 目前的規劃是 CSV 用於時間序列圖表，JSON 用於單一時間點統計，所以暫時不需要複雜的合併邏輯
    // 如果未來需要將兩者合併用於同一圖表，則需要更詳細的合併策略
    return csvData; // 暫時只返回 CSV 數據
}