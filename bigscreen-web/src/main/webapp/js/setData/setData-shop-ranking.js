shop_name = new Map();
shop_name.set("115974463", "天赐精品时尚男装");
shop_name.set("102781795", "战地特惠自营店");
shop_name.set("130175431", "gxgjeans官方店");
shop_name.set("118332811", "美嘉怡母婴");
shop_name.set("116372249", "都市丽人de时光店");
shop_name.set("61287947", "美孕の衣厨");
shop_name.set("107017127", "好奇小姐的内衣店");
shop_name.set("69971708", "朵朵DIY社区");
shop_name.set("5993647", "爱宜宜家居");
shop_name.set("35939500", "大象摄影公会");
shop_name.set("33558755", "北京恒宇汇众数码店");
shop_name.set("34443447", "金凤凰相机行");
shop_name.set("60898016", "天雅文化艺术中心");
shop_name.set("198482296", "丫头精品童装");
shop_name.set("118081421", "艾妮家居母婴店");
shop_name.set("111411936", "永结同心婚庆店");
shop_name.set("64784616", "凌云汽车饰品");
shop_name.set("295708596", "紫月小屋");
shop_name.set("102864304", "淘摄阁");
shop_name.set("116509992", "Glass Box 玻璃花房");

function loadWebSocket_shop_ranking() {

    //通过websocket去访问后台服务,发起了一个servlet的请求
    var web_socket = new WebSocket('ws://localhost:8080/web_socket/shop_ranking')

    web_socket.onerror = function (event) {
        onError(event)
    };
    web_socket.onopen = function (event) {
        onOpen(event)
    };
    web_socket.onmessage = function (event) {//前台接受后台发送过来的数据
        onMessage_shop_ranking(event,shop_name)
    };

    function onMessage_shop_ranking(event,shop_name) {

        var shop_ranking_div = document.getElementById("kfk_shop_ranking");
        var str = "";

        //返回json对象
        var sd = JSON.parse(event.data);
        for (var i = 0; i < sd.length; i++) {
            if (i > 5) break;
            var circle_str = null;
            if (i == 0) {
                circle_str = "#f8fb05";
            }
            else if (i == 1) {
                circle_str = "#fbc403";
            }
            else if (i == 2) {
                circle_str = "#c79b03";
            } else {
                circle_str = "";
            }
            //var shop_name = shop_name.get(sd[i].shop_id);
           // alert(">>>"+shop_name);
            str += "<div class='row' style='margin-bottom:5px;margin-top:10px;line-height:30px;'>" +
                "<div class='col-md-4' style='width: 15%;text-align:center;vertical-align:middle;line-height:30px;color: #3dd3f8'>" +

                "<div class='circle' style='background-color: " + circle_str + "'>" + (i + 1) + "</div>" +
                "</div>" +
                "<div class='col-md-4' style='width: 50%;background-color: rgba(8,158,195,0.2);text-align:left;color: #3dd3f8'>" + sd[i].shop_name + "</div>" +
                "<div class='col-md-4' style='width: 30%;text-align:center;line-height:30px;color: #3dd3f8'>" + sd[i].shop_amt_sum + "</div>" +
                "</div>";


        }
        shop_ranking_div.innerHTML = str;

    }

    function onOpen(event) {
        /**webSocket open之后，发送消息给message 服务并保持长链接的消息输出**/
        web_socket.send("shop_ranking");
    }

    function onError(event) {
        alert(event.data);
    }


}





