

function loadCao_bottom_left(myChart) {
    myChart.setOption(option_bottom_left);
}

var saleDate=[
    {name:'北京京都大酒店',age:132},
    {name:'北京京城大酒店',age:321},
    {name:'北京京都罗斯克大酒店',age:452},
    {name:'北京国际大酒店',age:190},
    {name:'北京君乐大酒店',age:176},
    {name:'北京奥思克米大酒店',age:376},
    {name:'北京华福大酒店',age:387},
];
//desc 倒序 asc 正序 固定写法 对 saleDate.age进行排序
saleDate.sort(getSortFun('asc','age'));
function getSortFun(order, sortBy) {
    var ordAlpah = (order == 'asc') ?'>' : '<';
    var sortFun = new Function('a', 'b', 'return a.'+ sortBy + ordAlpah + 'b.'+ sortBy + '?1:-1');
    return sortFun;
}
var datale=[], datale2=[];
for(var i=0; i<saleDate.length; i++){
    datale.push(saleDate[i].name)
    datale2.push(saleDate[i].age)
}

var option_bottom_left= {
    color: ['#4cc47d'],
    tooltip : {
        trigger: 'axis',
    },
    grid: {
        left: '13%',
        right: '4%',
        bottom: '3%',
        top:'5%',
        containLabel: true
    },
    yAxis : [
        {
            type : 'category',
            data : datale,
            textStyle:{
                verticalAlign:'bottom',
                lineHeight: 56,
            },
            axisLabel: {
                show: true,
                margin: -10,
                align: 'left',
                verticalAlign: 'bottom',
                padding: [0, 0, 15, 0]
            },
            axisTick: {
                show:false,
                alignWithLabel: true
            },
            axisLine:{
                show:false,
            }
        }
    ],
    xAxis : [
        {
            type : 'value',
            show:false
        }
    ],
    series : [
        {
            type:'bar',
            barWidth: '15%',
            itemStyle:{
                normal:{
                    barBorderRadius:[5],
                }
            },
            label: {
                normal: {
                    show: true,
                    position: 'right'
                }
            },
            data:datale2
        }
    ]
};