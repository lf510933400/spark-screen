function loadWebSocket_all_amt() {

    var web_socket = new WebSocket('ws://localhost:8080/web_socket/all_amt')
    //当访问的服务发生异常的时候，进行回调处理
    web_socket.onerror = function (event) {
        onError(event)
    };
    //前后台打开连接的时候调用，做一些资源的初始化操作
    web_socket.onopen = function (event) {
        onOpen(event)
    };
    //前后台建立起连接之后，用作传输数据的
    web_socket.onmessage = function (event) {
        onMessage_all_amt(event)
    };

    function onMessage_all_amt(event) {
        $("#sum").empty();
        var all_amt_div = document.getElementById("sum");
        var reValue = JSON.parse(event.data);
        all_amt_div.innerText = cc(reValue.toFixed(2));

    }

    function onOpen(event) {
        /**webSocket open之后，发送消息给message 服务并保持长链接的消息输出**/
        web_socket.send("all_amt");
    }

    function onError(event) {
        alert(event.data);
    }


}


//人民币，格式化
function cc(s) {
    if (/[^0-9\.]/.test(s)) return "invalid value";
    s = s.replace(/^(\d*)$/, "$1.");
    s = (s + "00").replace(/(\d*\.\d\d)\d*/, "$1");
    s = s.replace(".", ",");
    var re = /(\d)(\d{3},)/;
    while (re.test(s))
        s = s.replace(re, "$1,$2");
    s = s.replace(/,(\d\d)$/, ".$1");
    return s.replace(/^\./, "0.")
}





