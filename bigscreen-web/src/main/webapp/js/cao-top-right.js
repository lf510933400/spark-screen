
function loadCao_top_right(myChart) {
    myChart.setOption(option_top_right);
}


var option_top_right = {


    grid: {
        left: '3%',
        right: '4%',
        bottom: '10%',
        top:'10%',
        containLabel: true
    },
    xAxis : [
        {
            type : 'category',
            data : ['13:00', '13:05', '13:10', '13:15', '13:20', '13:25', '13:55','16:55'],
            axisTick: {
                alignWithLabel: true
            },

            axisLine : {
                //轴线样式
                lineStyle : {
                    //设置轴线宽度
                    width : 1,
                    //轴线颜色
                    color : 'rgba(1,134,198,0.8)'
                },
            },

            axisLabel : {
                //刻度样式
                textStyle : {
                    //文字透明
                    color : '#3dd3f8'
                }

            }

        },

    ],
    yAxis : [
        {
            // type : 'category',
            // data : ['10','20','30','40'],
            name:'(万)',
            nameTextStyle: {
                color: '#3dd3f8',

            },
            axisTick: {
                alignWithLabel: true
            },
            axisLabel : {
                //刻度样式
                textStyle : {
                    //文字透明
                    color : '#3dd3f8'
                }

            },
            axisLine : {
                //轴线样式
                lineStyle : {
                    //设置轴线宽度
                    width : 1,
                    //轴线颜色
                    color : 'rgba(1,134,198,0.8)'
                }
            },
            splitLine : {
                show:false,
                //分割线样式
                lineStyle : {
                    //分割线透明度

                    color : '#381258'
                }
            }
        }
    ],
    series : [
        {
            name:'直接访问',
            type:'bar',
            barWidth: '40%',
            data:[100, 30, 12, 35, 41, 22,30,60],
            label: {
                normal: {
                    show: true,
                    position: 'top',
                    color:'#fff'

                }
            }
        }

    ],
    label: {
        normal: {
            show: true,
            position: 'top',
            formatter: '{c}'
        }
    },
    itemStyle: {
        normal: {

            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [{
                offset: 0,
                color: 'rgba(29, 201,237, 1)'
            }, {
                offset: 1,
                color: 'rgba(42, 158,241, 0.4)'
            }]),
            shadowColor: 'rgba(0, 0, 0, 0.1)',
            shadowBlur: 10

        }
    }
};

