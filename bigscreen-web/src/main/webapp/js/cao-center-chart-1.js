
function loadCao_center_chart_1(myChart) {

    myChart.setOption(optioncenter_chart_1);
}
var now = new Date();
var len = 10;
var res = [];
while (len--) {
    res.unshift(now.toLocaleTimeString().replace(/^\D*/,''));
    now = new Date(now - 2000);
}
var res1 = [];
var res2 = [];
var len = 10;
while (len--) {
    res1.push((Math.random()*10 + 5).toFixed(1) - 0);
    res2.push((Math.random()*10 + 5).toFixed(1) + 10);
}
var xAxisData = res; //x轴数据
var yAxisData = res1; //y轴数据
var yAxisData2 = res2; //y轴数据
// 指定图表的配置项和数据
var optioncenter_chart_1 = {
    title : {
        text: '实时销售数据'
    },
    tooltip : {
        trigger: 'axis'
    },
    legend: {
        data:['销量1', '销量2']
    },
    toolbox: {
        show : true,
        feature : {
            //dataView : {show: true, readOnly: false},
            magicType : {show: true, type: ['line', 'bar']},
            restore : {show: true},
            saveAsImage : {show: true}
        }
    },
    dataZoom : {
        show : false,
        start : 0,
        end : 100
    },
    xAxis : [
        {
            type : 'category',
            boundaryGap : true,
            data:xAxisData
        }
    ],
    yAxis : [
        {
            type : 'value',
            scale: true,
            name : '销量',
            boundaryGap: [0.2, 0.2]
        }
    ],
    series : [
        {
            name:'销量1',
            type:'line',
            lineStyle: {
                normal: {
                    //color: '#4F2F4F',
                    width: 2
                    //type: solid
                },
            },
            data:yAxisData
        },
        {
            name:'销量2',
            type:'line',
            lineStyle: {
                normal: {
                    //color: '#4F2F4F',
                    width: 2
                    //type: solid
                },
            },
            data:yAxisData2
        }
    ]
};
var lastData = 11;
var lastData2;
var axisData;
clearInterval(app);
var app = {};
app.timeTicket = setInterval(function (){
    var url = "redis.json?value=value1";
    var paramData={};
    $.ajax({
        url: url,
        type: 'post',
        data: paramData,
        dataType: 'json',
        async:false,
        error:function(){
            console.log("get redis error!!!")
        },
        success: function(result){
            if(result != null){
                axisData = result.result.dateTime;
                lastData = result.result.sales;
                lastData2 = parseInt(lastData)+10;
            }
        }
    });

    // 动态数据接口 addData
    myChart.addData([
        [
            0,        // 系列索引
            lastData, // 新增数据
            false,     // 新增数据是否从队列头部插入
            false,    // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
            axisData //横轴数据
        ],
        [
            1,        // 系列索引
            lastData2, // 新增数据
            false,     // 新增数据是否从队列头部插入
            false,    // 是否增加队列长度，false则自定删除原有数据，队头插入删队尾，队尾插入删队头
            axisData //横轴数据
        ]
    ]);
}, 5000);
