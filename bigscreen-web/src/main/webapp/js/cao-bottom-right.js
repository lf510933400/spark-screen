function loadCao_bottom_right(myChart) {
    myChart.setOption(option_bottom_right);
}
var option_bottom_right = {
    "series": [
        {
            "center": [
                "35.0%",
                "50%"
            ],
            "radius": [
                "49%",
                "50%"
            ],
            "clockWise": false,
            "hoverAnimation": false,
            "type": "pie",
            "itemStyle": {
                "normal": {
                    "label": {
                        "show": true,
                        "textStyle": {
                            "fontSize": 15,
                            "fontWeight": "bold"
                        },
                        "position": "center"
                    },
                    "labelLine": {
                        "show": false
                    },
                    "color": "#07b1db",
                    "borderColor": "#07b1db",
                    "borderWidth": 13
                },
                "emphasis": {
                    "label": {
                        "textStyle": {
                            "fontSize": 15,
                            "fontWeight": "bold"
                        }
                    },
                    "color": "#07b1db",
                    "borderColor": "#07b1db",
                    "borderWidth": 15
                }
            },
            "data": [
                {
                    "value": 52.7,
                    "name": "男性 52.7%"
                },
                {
                    "name": " ",
                    "value": 47.3,
                    "itemStyle": {
                        "normal": {
                            "label": {
                                "show": false
                            },
                            "labelLine": {
                                "show": false
                            },
                            "color": "#07b1db",
                            "borderColor": "#07b1db",
                            "borderWidth": 0
                        },
                        "emphasis": {
                            "color": "#07b1db",
                            "borderColor": "#07b1db",
                            "borderWidth": 0
                        }
                    }
                }
            ]
        },
        {
            "center": [
                "75.0%",
                "50%"
            ],
            "radius": [
                "49%",
                "50%"
            ],
            "clockWise": false,
            "hoverAnimation": false,
            "type": "pie",
            "itemStyle": {
                "normal": {
                    "label": {
                        "show": true,
                        "textStyle": {
                            "fontSize": 15,
                            "fontWeight": "bold"
                        },
                        "position": "center"
                    },
                    "labelLine": {
                        "show": false
                    },
                    "color": "#ee3a3a",
                    "borderColor": "#ee3a3a",
                    "borderWidth": 13
                },
                "emphasis": {
                    "label": {
                        "textStyle": {
                            "fontSize": 15,
                            "fontWeight": "bold"
                        }
                    },
                    "color": "#ee3a3a",
                    "borderColor": "#ee3a3a",
                    "borderWidth": 25
                }
            },
            "data": [
                {
                    "value": 47.3,
                    "name": "女性 47.3%"
                },
                {
                    "name": " ",
                    "value": 52.7,
                    "itemStyle": {
                        "normal": {
                            "label": {
                                "show": false
                            },
                            "labelLine": {
                                "show": false
                            },
                            "color": "#ee3a3a",
                            "borderColor": "#ee3a3a",
                            "borderWidth": 0
                        },
                        "emphasis": {
                            "color": "#ee3a3a",
                            "borderColor": "#ee3a3a",
                            "borderWidth": 0
                        }
                    }
                }
            ]
        }
    ]
};