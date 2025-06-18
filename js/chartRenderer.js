// Chart.js 實例
let timeSeriesChartInstance = null;
let sourceDistributionChartInstance = null;

export function prepareTimeSeriesChartData(filteredData, fuelTypes, fuelColors) {
    const labels = filteredData.map(row => {
        const date = new Date(row.Time);
        return date.toLocaleString('zh-TW', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit' });
    });

    const datasets = fuelTypes.map(type => {
        return {
            label: type,
            data: filteredData.map(row => row[type] || 0),
            borderColor: fuelColors[type] || '#000000', // 預設黑色
            fill: false,
            tension: 0.1
        };
    });

    return { labels, datasets };
}

export function renderTimeSeriesChart(chartData, elementId, existingChartInstance) {
    const ctx = document.getElementById(elementId).getContext('2d');

    if (existingChartInstance) {
        existingChartInstance.data.labels = chartData.labels;
        existingChartInstance.data.datasets = chartData.datasets;
        existingChartInstance.update();
        return existingChartInstance;
    } else {
        return new Chart(ctx, {
            type: 'line',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: '各發電來源時間序列趨勢'
                    }
                },
                scales: {
                    x: {
                        type: 'category',
                        title: {
                            display: true,
                            text: '時間'
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: '發電量 (MW)'
                        },
                        beginAtZero: true
                    }
                }
            }
        });
    }
}

export function prepareSourceDistributionChartData(summaryData, fuelColors) {
    const labels = Object.keys(summaryData.fuelTypeTotals);
    const data = Object.values(summaryData.fuelTypeTotals).map(value => parseFloat(value));
    const backgroundColors = labels.map(label => fuelColors[label] || '#CCCCCC'); // 預設灰色

    return {
        labels: labels,
        datasets: [{
            data: data,
            backgroundColor: backgroundColors,
            hoverOffset: 4
        }]
    };
}

export function renderPieChart(chartData, elementId, existingChartInstance) {
    const ctx = document.getElementById(elementId).getContext('2d');

    if (existingChartInstance) {
        existingChartInstance.data.labels = chartData.labels;
        existingChartInstance.data.datasets = chartData.datasets;
        existingChartInstance.update();
        return existingChartInstance;
    } else {
        return new Chart(ctx, {
            type: 'pie',
            data: chartData,
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    title: {
                        display: true,
                        text: '發電來源佔比'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                let label = context.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed !== null) {
                                    label += context.parsed.toFixed(2) + ' MW (' + (context.parsed / chartData.datasets[0].data.reduce((a, b) => a + b, 0) * 100).toFixed(2) + '%)';
                                }
                                return label;
                            }
                        }
                    }
                }
            }
        });
    }
}