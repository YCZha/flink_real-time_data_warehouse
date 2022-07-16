package org.example.gmallreal.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.gmallreal.bean.TransientSink;
import org.example.gmallreal.common.GmallConfig;

import java.beans.Transient;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

//操作ClickHouse的工具类
/*
因为flink中对所有支持jdbc的数据库提供了一个工具包，Flink-connector中暴露出的sink接口，可以通过实现该接口
我们可以通过封装一个操作ClickHouse数据库的类实现该接口，因为ClickHouse数据库支持Jdbc
 */
public class ClickHouseUtil {
    public static <T>SinkFunction getJdbcSink(String sql){
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                sql,
                //执行写入操作,就是将当前流中的对象属性赋值给sql中的占位符
                //obj是流中的一条数据对象
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T obj) throws SQLException {
                        //具体的操作
                        //反射，获取当前类中 所有的属性
                        Field[] fields = obj.getClass().getDeclaredFields(); //这个方法可以获取私有属性，getFields只能获取公有属性
                        int skipOffset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            //还可以通过获取注解判断属性上是否有注解，如果有需要做什么处理(这里表示跳过)
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if(transientSink != null){
                                skipOffset++;
                                continue;
                            }
                            //设置私有属性可访问
                            field.setAccessible(true);
                            try {
                                //获取属性值对象
                                Object o = field.get(obj); //反射的操作方法  属性.get(对象)，表示获取该对象的该属性的值， 方法.invoke(对象)，表示调用对象的方法
                                preparedStatement.setObject(i + 1 - skipOffset, o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性复制，执行批次大小
                new JdbcExecutionOptions.Builder().withBatchIntervalMs(5).build(),
                //构建者设计模式，创建JdbcConnectionOption对象，给连接相关属性复制
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
        return sinkFunction;
    }
}
