
function loadCao_bottom_right_1(myChart) {
    myChart.setOption(option_bottom_right_1);
}

// 值
var valueOne = 80;
var valueTwo = 50;
var valueThree = 40;
var unit = "%";
var min = 0;
var max = 200;
var color = ['#e92101','#e74701','#f1d301'];
var size = '100%';
var background = "#0e2147"; //背景
// 控制位置
var centerOne = ['25%', '45%'];
var centerThree = ['75%', '65%'];
// 控制大小
// 外圆
var radiusCncircle = ['55%', '56%'];
// 内圆
var radiusInCircle = ['43%', '44%'];
var dataStyle = {
    normal: {
        label: {
            show: false
        },
        labelLine: {
            show: false
        },
    }
};
var placeHolderStyle = {
    normal: {
        color: '#18346d', //未完成的圆环的颜色
        label: {
            show: false
        },
        labelLine: {
            show: false
        }
    }
};
var option_bottom_right_1 = {
    //backgroundColor: background,
    series: [{
        name: '外圆1',
        type: 'pie',
        clockWise: false,
        center:centerOne,
        radius: radiusCncircle,
        itemStyle: dataStyle,
        hoverAnimation: false,
        avoidLabelOverlap: false,
        label:{
            normal:{
                show:true,
                position:'center',
                formatter:valueOne + unit+'\n\n'+'男性',
                textStyle:{
                    color:'#ffffff',
                    fontSize: 18
                }
            }
        },
        data: [{
            value: valueOne - min,
            name: '01',
            itemStyle:{
                normal:{
                    color:color[0]
                }
            }
        }, {
            value: max - valueOne,
            name: 'invisible',
            itemStyle: placeHolderStyle
        }

        ]
    }, {
        name: '内圆1',
        type: 'pie',
        animation: false,
        clockWise: false,
        center:centerOne,
        radius: radiusInCircle,
        itemStyle: dataStyle,
        hoverAnimation: false,
        data: [{
            value: 100,
            name: '02',
            itemStyle: {
                normal: {
                    color: '#18346d'
                }
            }
        }, {
            value: 0,
            name: 'invisible',
            itemStyle: placeHolderStyle
        }

        ]
    },

        {
            name: '外圆3',
            type: 'pie',
            clockWise: false,
            center:centerThree,
            radius: radiusCncircle,
            itemStyle: dataStyle,
            hoverAnimation: false,
            avoidLabelOverlap: false,
            label:{
                normal:{
                    show:true,
                    position:'center',
                    formatter:valueThree + unit+'\n\n'+'女性',
                    textStyle:{
                        color:'#ffffff',
                        fontSize: 18
                    }
                }
            },
            data: [{
                value: valueThree - min,
                name: '01',
                itemStyle:{
                    normal:{
                        color:color[2]
                    }
                }
            }, {
                value: max - valueThree,
                name: 'invisible',
                itemStyle: placeHolderStyle
            }

            ]
        }, {
            name: '内圆3',
            type: 'pie',
            animation: false,
            clockWise: false,
            center:centerThree,
            radius: radiusInCircle,
            itemStyle: dataStyle,
            hoverAnimation: false,
            data: [{
                value: 100,
                name: '02',
                itemStyle: {
                    normal: {
                        color: '#18346d'
                    }
                }
            }, {
                value: 0,
                name: 'invisible',
                itemStyle: placeHolderStyle
            }

            ]
        }]
};