
function zeroAdd(num, n) {
    return (Array(n).join(0) + num).slice(-n);
}
var myChart8 = echarts.init(document.getElementById('div-center'));

var dtCurDate = new Date();
var arrCurTime = [];

//var dtTimeAM = new Date(dtCurDate.getFullYear(), dtCurDate.getMonth(), dtCurDate.getDate(), dtCurDate.getHours(), dtCurDate.getMinutes(), 0, 0)
var dtTimeAM = new Date(dtCurDate.getFullYear(), dtCurDate.getMonth(), dtCurDate.getDate(), 0,0, 0, 0)


for (var i=0; i<=24*60*60+100; i++)
{
    arrCurTime.push([dtTimeAM.getHours(), dtTimeAM.getMinutes()].join(":"));
    dtTimeAM = new Date(dtTimeAM.getTime() +  1000);
}

//alert(JSON.stringify(arrCurTime.length));


//***************************************************************//

var line_data = [];
for (var i=0; i<arrCurTime.length; i++)
{
    line_data.push( {name: arrCurTime[i], value: "-"} );
}

//alert(line_data.length / 10);


function getDataPos(time)
{
    for (var i=0; i<line_data.length; i++)
    {
        if (time == line_data[i].name) return i;
    }
}


var color = '#189cbb';
var lineColor = '#0a324b'

var scale = 1;

var option8 = {
    //backgroundColor: '#031f2d',
    color: ['#e50505','#ffc72b'], //3dd3f8
    grid: {
        left: '8%',
        right: '10%',
        top:'20%',
        bottom: '5%',
        containLabel: false
    },


    xAxis : {
        boundaryGap : false,
        type : 'category',


        data: arrCurTime,
        //scale: true,


        axisLine: {
            lineStyle: {
                color: '#3dd3f8',
                width:2

            }
        },
        axisLabel : {
            interval : 7200,
            //show : true,
            textStyle: {
                color: color,
                fontSize: 16*scale,
            },
            formatter: function (value, index) {

                //alert(value);
               // var now_value =zeroAdd(value.split(":")[0],2) + ":"+zeroAdd(value.split(":")[1],2);
                var now_value =zeroAdd(value.split(":")[0],2);
                //alert(index);
                if(now_value == '00' && index > 1){
                    //alert(value);
                    now_value = 24;
                }

                return now_value;
            }

        },
        name: '(时间)',
        nameTextStyle: {
            color: '#3dd3f8',
            fontSize: 16*scale,
            padding: [0, 0, 0, 10]
        },
    },
    yAxis : [{
        splitNumber:5,
        // max: function(value) {
        //     return value.max + 20000;
        // },
        type:'value',
        max:5000,
        scale : false,
        axisTick:{
            length:5
       },
        axisLabel: {
            formatter:function (value,index) {
              if(value == 0){
                  return "";
              }else{
                  return value;
              }
            },
            padding: [0, 1, 0, 0],
            textStyle: {
                color: '#43d7fb',
                fontSize: 16*scale,
            }
        },
        axisLine: {
            show:false,
            lineStyle: {
                //color: '#3dd3f8',
                color:'#160a70'
            }
        },
        splitLine: {
            show: true,
            lineStyle: {

                color:'#186173',
                type:'dotted'
            }
        },
        name: '成交额(万)',
        nameTextStyle: {
            color: '#3dd3f8',
            fontSize: 16*scale,
            padding: [0, 0, 25, 0]
        },
    }],
    series : [{
        name : '当前交易金额',
        type:'line',
        smooth: true,
        data : line_data,
        symbol: 'none', //拐点样式
        markPoint: {
            data: [{
                type: 'max',
                symbolSize: [70, 65],
                itemStyle: {
                    normal: {
                        color: "#e50505"
                    }
                },
                label: {
                    normal: {
                        formatter: '{c}',
                        show: true,
                        textStyle: {
                            fontSize: '10',
                            //fontWeight: 'bold',
                            color: '#fff'
                        }
                    }
                }
            },
                {
                    symbol:'circle',
                    type: 'max',
                    symbolSize: [10, 10],
                    itemStyle: {
                        normal: {
                            color: "#e50505",
                            borderColor:'rgba(235,41,125,0.3)',//rgba(255, 199, 43, .3)
                            borderWidth:10*scale,
                            shadowColor:'#e50505',
                            shadowBlur:30
                        }
                    },
                    label: {
                        normal: {
                            show: false
                        }
                    }
                }]
        },
        markLine: {
            symbol: 'none',
            data: [{
                yAxis:60000,
                valueIndex: 1,
                // coord:['60','7'],
                lineStyle: {
                    normal: {
                        color: '#3fdaff',
                        type: 'dashed'
                    }
                },
                label: {
                    normal: {
                        show: false
                    }
                }
            }]
        }

    }]
};

// 使用刚指定的配置项和数据显示图表。
myChart8.setOption(option8);

//
// var app = {};
// app.timeTicket = setInterval(function () {
//
//     getLineData();
//
//     //  clearInterval(app.timeTicket);
//
// }, 1000);

// window.onresize = function(){
//     // alert("asdfasdfasdf");
//     myChart8.resize();
// };


var cao_count = 0;
var json = [];

function getLineData(count,time_pos) {

    //alert(count);
    ///var yvalue =zeroAdd(time_pos.split(":")[0],2) + ":"+zeroAdd(time_pos.split(":")[1],2);
   // alert(time_pos);
    //alert(JSON.stringify(line_data));
    //alert(yvalue);
   // var pos = getDataPos(yvalue);
   // alert(pos);
    // 替换数组对应点
    //if (pos == undefined) return false;
    //line_data.splice(pos, 1, {name: yvalue, value: count});
    //



    json.push({name: time_pos, value: count});

    myChart8.setOption({
        series: [{
            data: json
        }]
    });

}