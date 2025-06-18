export function initDatePickers() {
    const startDateInput = document.getElementById('startDate');
    const endDateInput = document.getElementById('endDate');

    // 設置預設日期為今天和昨天
    const today = new Date();
    const yesterday = new Date(today);
    yesterday.setDate(today.getDate() - 1);

    startDateInput.value = yesterday.toISOString().split('T')[0];
    endDateInput.value = today.toISOString().split('T')[0];
}

export function getFilterDates() {
    const startDate = document.getElementById('startDate').value;
    const endDate = document.getElementById('endDate').value;
    return { startDate, endDate };
}

export function filterDataByDateRange(data, startDateStr, endDateStr) {
    const startDate = new Date(startDateStr + 'T00:00:00');
    const endDate = new Date(endDateStr + 'T23:59:59');

    return data.filter(row => {
        const rowDate = new Date(row.Time);
        return rowDate >= startDate && rowDate <= endDate;
    });
}

export function initTimePointSelector() {
    const timePointSelector = document.getElementById('timePointSelector');
    const now = new Date();
    // 設置預設時間點為當前時間，但分鐘數調整為最接近的10分鐘倍數
    const minutes = now.getMinutes();
    const roundedMinutes = Math.floor(minutes / 10) * 10;
    now.setMinutes(roundedMinutes, 0, 0); // 設置秒和毫秒為0

    // 格式化為 YYYY-MM-DDTHH:MM (24小時制)
    const year = now.getFullYear();
    const month = (now.getMonth() + 1).toString().padStart(2, '0');
    const day = now.getDate().toString().padStart(2, '0');
    const hours = now.getHours().toString().padStart(2, '0');
    const mins = now.getMinutes().toString().padStart(2, '0');

    timePointSelector.value = `${year}-${month}-${day}T${hours}:${mins}`;
}

export function getSelectTimePoint() {
    return document.getElementById('timePointSelector').value;
}