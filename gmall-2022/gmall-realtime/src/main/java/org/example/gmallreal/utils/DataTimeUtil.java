package org.example.gmallreal.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * SimpleDateFormat存在线程安全问题
 * sdf做format操作是，会调用calendar.setTime方法，此方法会修改成员变量，如果多线程访问，成员变量没有加锁，会导致线程安全问题
 * 解决方法：JDK8之后提供了DateTimeFormat替代SimpleDateFormat
 *
 */

//日期转换工具类
public class DataTimeUtil {
//    public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String toYMDhms(Date date){
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    public static Long toTS(String dataStr){
        LocalDateTime localDateTime = LocalDateTime.parse(dataStr, dtf);
        Long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;
    }
}
