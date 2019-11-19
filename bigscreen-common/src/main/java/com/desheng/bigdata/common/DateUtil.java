package com.desheng.bigdata.common;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {
    public static DateFormat df_standard = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");



    public static long parseTime(String timeStr) {
        try {
            return df_standard.parse(timeStr).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return -1L;
    }

    public static String formatTime(Date date) {
        return df_standard.format(date);
    }
}
