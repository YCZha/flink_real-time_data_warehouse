package org.example.gmallreal.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.gmallreal.common.GmallConfig;
import org.example.gmallreal.utils.DimUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;

//处理维度数据的Sink实现类
public class DimSink extends RichSinkFunction<JSONObject> {
    //声明phoenix连接对象、
    private Connection conn = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        //对连接对象进行初始化
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //对一条json数据进行处理，将data中的数据插入到hbase表中
    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取目标表的名称
        String tableName = jsonObj.getString("sink_table");
        //经过过滤之后的保留的业务表字段
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        if(dataJsonObj != null && dataJsonObj.size() > 0){
            //根据data中的属性名和属性值，生成upsert操作
            String upsertSql = getUpsert(tableName.toUpperCase(), dataJsonObj);
            System.out.println("向hbase中插入数据：" + upsertSql);

            PreparedStatement ps = null;
            try {
                //执行sql
                ps = conn.prepareStatement(upsertSql);
                ps.execute();
                //注意，执行完插入操作，需要手动提交事务(mysql事务是自动提交的，phoenix是关闭自动提交的)
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("向phoenix插入数据失败");
            }finally {
                if(ps != null){
                    ps.close();
                }
            }
            //如果做的是更新操作，将redis中缓存数据清除掉
            if(jsonObj.getString("type").equals("update")){
                DimUtil.deleteCached(tableName, dataJsonObj.getString("id"));
            }
        }

    }
    //根据data属性和值，生成phoenix插入数据的sql语句
    private String getUpsert(String tableName, JSONObject dataJsonObj) {
        //获取json对象中的key,和value
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();
        //"upsert into 表空间.表名（列名....） values(值.....)"
        String upsertSql = "upsert into "+GmallConfig.HBASE_SCHEMA+"."+ tableName+"("+
                StringUtils.join(keys, ",") + ")";
        //因为属性值是varchar类型，所以需要用‘’将属性值括起来
        String valueSql = " values ('"+ StringUtils.join(values,"','")+"')";
        return upsertSql + valueSql;
    }
}
