
function loadCao_center_right(myChart) {
    myChart.setOption(option_center_right);
}


var item = ["重庆", "武汉", "温州", "广州", "深圳", "北京",  "上海", "杭州"];
var value = [ 6681, 8899, 9695, 10551, 10567, 11924,19345, 23260];
var option_center_right = {


    // 调色，


    // 提示框
    tooltip: {
        trigger: 'axis',
        axisPointer: {
            type: 'shadow'
        },
        textStyle: {
            fontSize: 2
        }
    },
    legend: {
        show: false
    },
    grid: {
        left: '4%',
        right: '16%',
        top: '2%',
        bottom: '0%',
        containLabel: true
    },
    xAxis: {
        show: false,
        boundaryGap: [0, 0.5]


    },
    yAxis: {
        type: 'category',
        // 轴线
        axisLine: {
            show: false,
            boundaryGap: true
        },
        // 刻度
        axisTick: {
            show: false,
            alignWithLabel: true,
            boundaryGap: true
        },
        axisLabel: {
            show: true,
            margin: 25,
            textStyle: {
                fontSize: 12,
                color : '#3dd3f8'
            },
            formatter: function(name) {
                // 当名称长度超过10,进行回行处理且循环持续, 防止过长
                var result = '';
                for (var i = 0, len = name.length; i < len; i++) {
                    result += name[i];
                    if (i >= 9 && (i % 9 === 0)) {
                        result += '\n';
                    }
                }
                return result;
            }
        },
        data: item
    },

    series: [{
        type: 'bar',
        barCategoryGap: '60%',
        barWidth: 10,
        label: {
            normal: {
                color:'#fff',
                show: true,
                position: 'right',
                // 图示标签显示内容格式化
                formatter: function(item) {
                    return Number(item.value).toFixed(0);
                }
            }
        },
        itemStyle: {
            normal: {
                color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [{
                    offset: 0,
                    color: 'rgba(29, 201,237, 1)'
                }, {
                    offset: 1,
                    color: 'rgba(42, 158,241, 0.8)'
                }], false),
                barBorderRadius: [0, 50, 50, 0],
                shadowColor: 'rgba(0,0,0,0.1)',
                shadowBlur: 3,
                shadowOffsetY: 3
            }
        },
        zlevel: 20,
        data: value
    }]
};