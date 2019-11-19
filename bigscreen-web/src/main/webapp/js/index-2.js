
function loadInit() {

    // 基于准备好的dom，初始化echarts图表
    var div_center_right = document.getElementById('div-center-right');


    // var div_top_left = document.getElementById('div-top-left');

    var cao_charts_right_1 = document.getElementById('cao_charts_right_1');
    var cao_charts_right_2 = document.getElementById('cao_charts_right_2');
    var cao_charts_right_3 = document.getElementById('cao_charts_right_3');



    var cao_charts_left_1 =document.getElementById('cao_charts_left_1');
    var cao_charts_left_2 = document.getElementById('cao_charts_left_2');
    var cao_charts_left_3 = document.getElementById('cao_charts_left_3');


    var myChartContainer = function () {

        div_center_right.style.height = (window.innerHeight/3 - 50)+'px';
        cao_charts_right_3.style.height = (window.innerHeight/3 - 70)+'px';
        cao_charts_right_1.style.height = (window.innerHeight/3 - 50)+'px';
        //div_top_left.style.height = (window.innerHeight/3 - 50)+'px';


        cao_charts_right_2.style.height = (window.innerHeight/3 - 50)+'px';
        cao_charts_left_1.style.height = (window.innerHeight/3 - 50)+'px';
        cao_charts_left_2.style.height = (window.innerHeight/3 - 50)+'px';
        cao_charts_left_3.style.height = (window.innerHeight/3 - 70)+'px';

    };

    myChartContainer();
    var my_cao_charts_right_1 = echarts.init(cao_charts_right_1);
    var center_right = echarts.init(div_center_right);
    var myChart_bottom_right = echarts.init(cao_charts_right_3);

    var myChart_center_left = echarts.init(cao_charts_left_2);




    // 为echarts对象加载数据
    loadCao_top_right(my_cao_charts_right_1);
    loadCao_center_right(center_right);
    loadCao_bottom_right( myChart_bottom_right);
    loadCao_center_left(myChart_center_left);
    window.onresize = function () {
        myChartContainer();
        center_right.resize();
    };


}

var count =0;
var startTime = new Date();
window.setInterval("clock()",1000);

var jishu = 0;
function clock()
{


    jishu +=parseInt(1);

    var now = new Date();
    var time_pos = now.getHours() + ":" + now.getMinutes() +":"+now.getSeconds() ;

    var dd = parseInt(startTime.getTime() + 10*6000);



    if(jishu > 400 && jishu < 1000){
        count +=parseFloat((Math.random() * 5000).toFixed(2)) ;
    }
    else if(jishu >1000 && jishu < 1500){

        count +=parseFloat((Math.random() * 2000).toFixed(2)) ;
    }
    else if(jishu >1500 && jishu < 2000){

        count +=parseFloat((Math.random() * 5000).toFixed(2)) ;
    }
    else if(jishu >2000 && jishu < 2500){

        count +=parseFloat((Math.random() * 2000).toFixed(2)) ;
    }
    else if(jishu >2500 && jishu < 3000){

        count +=parseFloat((Math.random() * 300).toFixed(2)) ;
    }
    else if(jishu >3000){

        count +=parseFloat((Math.random() * 550).toFixed(2)) ;
    }

    else{
        count +=parseFloat((Math.random() * 10000).toFixed(2)) ;
    }

    $("#sum").empty();
    //count =parseInt(document.getElementById("sum").innerText);
    var sum = document.getElementById("sum");

    //alert(now.getMinutes());

    //count = count.toFixed(2);
    sum.innerText = cc(count.toFixed(2));

    //var nowTime =now.toLocaleTimeString().replace(/^\D*/,'');
    //setLines(count,nowTime);



    //if(jishu == 20 || jishu == 30){

    getLineData((count/10000).toFixed(2),time_pos);
    //getLineData((count/1000).toFixed(2),'-');
    //}


}


//人民币，格式化
function cc(s){
    if(/[^0-9\.]/.test(s)) return "invalid value";
    s=s.replace(/^(\d*)$/,"$1.");
    s=(s+"00").replace(/(\d*\.\d\d)\d*/,"$1");
    s=s.replace(".",",");
    var re=/(\d)(\d{3},)/;
    while(re.test(s))
        s=s.replace(re,"$1,$2");
    s=s.replace(/,(\d\d)$/,".$1");
    return s.replace(/^\./,"0.")
}







