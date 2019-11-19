package com.desheng.bigdata.bigscreen.web;


import com.desheng.bigdata.bigscreen.service.ShopService;

import javax.websocket.*;
import javax.websocket.server.ServerEndpoint;
import java.io.IOException;

/**
 */
@ServerEndpoint("/web_socket/all_amt")
public class AllAmtSocket {


    @OnMessage
    public void onMessage(String message, Session session)
            throws IOException, InterruptedException {

        while(true){
            //JSON串
            String returnStr = String.valueOf(ShopService.getAllAmt());
            System.out.println("all_amt>>>>"+returnStr);
            session.getBasicRemote().sendText(returnStr);
            Thread.sleep(1000);
        }
    }

    @OnError
    public void onError(Session session, Throwable error){
        System.out.println("发生错误");
        error.printStackTrace();
    }



    @OnOpen
    public void onOpen () {
        System.out.println("连接到了服务了********** Client connected");
    }
    @OnClose
    public void onClose () {
        System.out.println("Connection closed");
    }

}
