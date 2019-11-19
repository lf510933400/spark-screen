package com.desheng.bigdata.bigscreen.util;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 */
public class DateUtil {

    public static String getCurrTime(){

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
        Date curDate = new Date(System.currentTimeMillis());//获取当前时间
        return formatter.format(curDate);

    }

    public static String getLazyCurrTime(int lazy){

        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
        Date curDate = new Date(System.currentTimeMillis() - lazy);//获取当前时间
        return formatter.format(curDate);

    }

    public static void main(String[] args) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd-HH:mm:ss");
        Date curDate = new Date(System.currentTimeMillis());//获取当前时间
        System.out.println(formatter.format(curDate));
    }
}
