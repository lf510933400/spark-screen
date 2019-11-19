function loadWebSocket_x_time() {

    var web_socket = new WebSocket('ws://localhost:8080/web_socket/x_time')

    web_socket.onerror = function (event) {
        onError(event)
    };
    web_socket.onopen = function (event) {
        onOpen(event)
    };
    web_socket.onmessage = function (event) {
        onMessage_x_time(event)
    };

    function onMessage_x_time(event) {

        var sd = JSON.parse(event.data);

        var x_time = sd.x_time;
        var time_amtSum = sd.time_amtSum;

        //yyyy-MM-dd HH:ss:mm
        var _x_time = x_time.substr(9,x_time.length)//HH:ss:mm
        //alert(_x_time + ":"+shop_amtSum);
        getLineData((time_amtSum).toFixed(2), _x_time);

    }

    function onOpen(event) {
        /**webSocket open之后，发送消息给message 服务并保持长链接的消息输出**/
        web_socket.send("x_time");
    }

    function onError(event) {
        alert(event.data);
    }


}





