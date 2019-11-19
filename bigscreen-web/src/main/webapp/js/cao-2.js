
// 路径配置
require.config({
    paths: {
        echarts: 'http://echarts.baidu.com/build/dist'
    }
});

// 使用
require(
    [
        'echarts',
        'echarts/chart/bar',
        'echarts/chart/line'

        // 使用柱状图就加载bar模块，按需加载
    ],
    function (ec) {
        // 基于准备好的dom，初始化echarts图表
        var myChart_1 = ec.init(document.getElementById('div-1'));
        var myChart_3 = ec.init(document.getElementById('div-3'));

        var myChart_6 = ec.init(document.getElementById('div-6'));
        var myChart_7 = ec.init(document.getElementById('div-7'));
        var myChart_8 = ec.init(document.getElementById('div-8'));




        var option_2 = {
            backgroundColor:'',//设置无背景色
            //backgroundColor:'rgba(251,217,97,0.5)',
            grid : { // 控制图的大小，调整下面这些值就可以，
                y : 25,
                x : 90,
                x2 : 60,
                y2 : 60,// y2可以控制 X轴跟Zoom控件之间的间隔，避免以为倾斜后造成 label重叠到zoom上
                //去除边框线
                borderWidth : 0,
                backgroundColor:'rgba(251,217,97,0.5)',
            },

            title : {
                text: '某楼盘销售情况',
                subtext: '纯属虚构',
                textStyle:{color: '#d1d2d3'},
                subtextStyle:{color:'#b0b1b1'}

            },
            tooltip : {
                trigger: 'axis',
                textStyle:{color:'#b0b1b1'}
            },
            legend: {
                data:['意向','预购','成交'],
                textStyle:{color:'#b0b1b1'}
            },

            calculable : true,
            xAxis : [
                {
                    type : 'category',
                    boundaryGap : false,
                    data : ['周一','周二','周三','周四','周五','周六','周日'],
                    itemStyle: {
                        normal: {
                            //填充区域
                            areaStyle : {
                                //默认填充
                                type : 'default',
                                //填充颜色
                                color : 'rgba(251,217,97,0.4)'
                            },
                            //线条颜色
                            color : 'rgba(236, 208, 192,0.8)'
                        }
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
                            color : 'rgba(255,255,255,0.5)'
                        }

                    },

                    //分割线
                    splitLine : {
                        //分割线样式
                        lineStyle : {
                            //分割线透明度
                            color : ['rgba(255,255,255,0)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)']
                        }
                    },
                    axisTick : {
                        //隐藏刻度线
                        show : false
                    }

                }
            ],
            yAxis : [
                {
                    type : 'value',
                    axisLabel : {
                        //刻度样式
                        textStyle : {
                            //文字透明
                            color : 'rgba(255,255,255,0.5)'
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

                    //分割线
                    splitLine : {
                        //分割线样式
                        lineStyle : {
                            //分割线透明度
                            color : ['rgba(255,255,255,0)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)',
                                'rgba(255,255,255,0.3)','rgba(255,255,255,0.3)','rgba(255,255,255,0.3)'
                                ,'rgba(255,255,255,0)']
                        }
                    }

                }
            ],

            grid:{
                borderColor:'#032a42'
            },
            series : [
                {
                    name:'成交',
                    type:'line',
                    smooth:true,
                    itemStyle: {
                        normal: {
                            //填充区域
                            areaStyle : {
                                //默认填充
                                type : 'default',
                                //填充颜色
                                color : 'rgba(28,148,139,0.4)'
                            },
                            //线条颜色
                            color : 'rgba(2, 208, 192,0.8)'
                        }
                    },
                    data:[10, 12, 21, 54, 260, 830, 710]
                },
                {
                    name:'预购',
                    type:'line',
                    smooth:true,
                    itemStyle: {
                        normal: {
                            //填充区域
                            areaStyle : {
                                //默认填充
                                type : 'default',
                                //填充颜色
                                color : 'rgba(53,227,5,0.4)'
                            },
                            //线条颜色
                            color : 'rgba(2, 208, 192,0.8)'
                        }
                    },
                    data:[30, 182, 434, 791, 390, 30, 10]
                },
                {
                    name:'意向',
                    type:'line',
                    smooth:true,
                    itemStyle: {
                        normal: {
                            //填充区域
                            areaStyle : {
                                //默认填充
                                type : 'default',
                                //填充颜色
                                color : 'rgba(2,97,202,0.4)'
                            },
                            //线条颜色
                            color : 'rgba(2, 208, 192,0.8)'
                        }
                    },
                    data:[1320, 1132, 601, 234, 120, 90, 20]
                }
            ]
        };




        // 为echarts对象加载数据
        myChart_1.setOption(option_2);
        myChart_3.setOption(option_2);

        myChart_6.setOption(option_2);
        myChart_7.setOption(option_2);
        myChart_8.setOption(option_2);


    }
);

