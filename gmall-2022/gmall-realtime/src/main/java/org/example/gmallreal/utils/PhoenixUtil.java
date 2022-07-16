package org.example.gmallreal.utils;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.beanutils.BeanUtils;
import org.example.gmallreal.common.GmallConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//从phoenix中查询数据
public class PhoenixUtil {
    private static Connection conn = null;

    public static void init(){
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver"); //获取注册驱动
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER); //获取phoenix连接
            conn.setSchema(GmallConfig.HBASE_SCHEMA); //指定表空间，这样在执行查询语句时就不需要指定表空间
            System.out.println("获取phoenix连接成功！");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    //从phoenix中查询数据
    public static <T> List<T> queryList(String sql, Class<T> clz){
        if(conn == null){
            init();
        }
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //获取数据库操作对象
             ps = conn.prepareStatement(sql);
            //执行SQL语句
             rs = ps.executeQuery();
             //通过结果集对象获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //处理结果集
            while (rs.next()){
                //要声明一个对象，用于封装查询的一条结果集
                T obj = clz.newInstance();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    BeanUtils.setProperty(obj,metaData.getColumnName(i),rs.getObject(i));
                }
                resultList.add(obj);
            }


        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从维度表查询数失败");
        }finally {
            //释放资源
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }

    public static void main(String[] args) {
        //执行时间非常慢，其实一部分原因是因为要进行初始化连接工作
        System.out.println(queryList("select * from DIM_USER_INFO", JSONObject.class));
        System.out.println("第二次查询》》》》》》》》》》》》》》》》》》》》》》》");
        //第二次查询时间间隔很短，说明时初始化连接的问题
        System.out.println(queryList("select * from DIM_USER_INFO", JSONObject.class));
    }
}
