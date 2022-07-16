package org.example.gmallreal.utils;


import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.example.gmallreal.bean.TableProcess;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//从mysql数据库中查询数据的工具类
//完成ORM，对象关系映射
//Java中的对象和关系型数据库中的表中的记录建立起映射关系————Mybatis
//将数据库中的表t_student和java中的类Student建立映射关系
//即需要将数据库中查到的ResultSet中的一条条记录映射为Java List中的一个个对象
public class MySQLUtil {
    /**
     *
     * @param sql 要执行的查询语句
     * @param clz 返回的数据类型
     * @param underScoreToCamel 是否要将下划线转化成驼峰命名法
     * @param <T>
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) {
        ArrayList<T> resultList = new ArrayList<>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            //TODO 1.注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //TODO 2.创建连接
            conn = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/gmall0622_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "980411");
            //TODO 3.创建数据库操作对象
            ps = conn.prepareStatement(sql);
            //TODO 4.执行SQL语句
            rs = ps.executeQuery();
            //查询结果元数据信息，拿到列的名称
            ResultSetMetaData metaData = rs.getMetaData();

            //TODO 5.处理结果集
            //判断结果集中是否存在数据，取出一条结果集数据
            while (rs.next()){
                //将取出的数据封装为一个对象
                T obj = clz.newInstance();
                //对查询到的一条结果集的数据进行每一列遍历
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = "";
                    if(underScoreToCamel){
                        //将下划线命名转化为驼峰命名，通过gava工具类
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    else{
                        //如果不为true,就使用下划线命名，一般建议使用驼峰命名
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE,columnName);
                    }
                    //调用commons-bean中的工具类，给obj属性复制
                    BeanUtils.setProperty(obj,propertyName,rs.getObject(i));
                }
//                将当前rs中的一行数据封装的obj对象存入List集合中
                resultList.add(obj);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Mysql查询数据失败");
        }finally {
            //TODO 6.释放资源
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
            if(conn != null){
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }

    public static void main(String[] args){
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess);
        }
    }
}
