package org.example.gmallreal.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

//用于维度查询的工具类，底层调用PhoenixUtil
public class DimUtil {
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String> ... cloNameAndValue){
        //拼接查询条件
        //因为是做维度关联，所以where不能为空，所以后面一定得跟条件表达式
        String whereSql = " where ";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String fieldName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if(i > 0){
                whereSql += " and ";
            }
            whereSql += fieldName + "='" + fieldValue + "'";
        }
        String sql = "select * from " + tableName + whereSql;
        System.out.println("查询维度的sql:" + sql);
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        //对于维度查询，一般根据主键进行查询，不可能返回多条记录，只会有一条
        JSONObject dimJSONObj = null;
        if(dimList != null && dimList.size() > 0){
            dimJSONObj = dimList.get(0);
        }
        else {
            System.out.println("维度数据没有找到：" + sql);
        }
        return dimJSONObj;
    }

    //在做维度关联的时候，大部分都是通过id进行关联，所以提供一个方法，只需将id的值作为参数传递进来
    public static JSONObject getDimInfoCache(String tableName, String id){ //方法重载
        return getDimInfoCache(tableName, Tuple2.of("id",id));
    }

    //优化，从phoenix中查询数据，加入了旁路缓存，先从缓存查询，如果缓存没有查到数据，再到phoenix查询，并将查询结果放到缓存中
    /*
    redis:
        类型： string（使用jsonStr形式） list set zset hash
        key: dim:表名:值   dim:dim_base_trademark:10
        value: 通过PhoenixUtil到维度表中查询数据，取出第一条并将其转化为json字符串
        失效时间 : 不能常驻内存，设置一天的失效时间：24*3600
     */
    public static JSONObject getDimInfoCache(String tableName, Tuple2<String, String> ... cloNameAndValue){
        //拼接查询条件
        //因为是做维度关联，所以where不能为空，所以后面一定得跟条件表达式
        String whereSql = " where ";
        //对redis的key进行拼接
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String fieldName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if(i > 0){
                whereSql += " and ";
                redisKey += "_"; //默认编程时不会弄乱程序
            }
            whereSql += fieldName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }

        //从redis中获取数据
        Jedis jedis = null;
        //维度数据的json字符串形式
        String dimJsonStr = null;
        //维度数据的json对象形式
        JSONObject dimJsonObj = null;
        try {
            //获取redis客户端
            jedis = RedisUtil.getJedis();
            //根据key到redis中查询
            dimJsonStr = jedis.get(redisKey);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从redis查询维度失败");
        }

        //是否从redis中查询到了数据
        if(dimJsonStr != null && dimJsonStr.length() > 0){
            dimJsonObj = JSON.parseObject(dimJsonStr);
        }else{
            System.out.println("redis中不存在key:"+redisKey+"，需要到hbase中载入");
            //如果在redis中没有查到数据，需要到phoenix中查询,并缓存到redis中
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的sql:" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度查询，一般根据主键进行查询，不可能返回多条记录，只会有一条
            if(dimList != null && dimList.size() > 0){
                dimJsonObj = dimList.get(0);
                //将查询到的数据放到redis中缓存起来
                if(jedis != null){
                    jedis.setex(redisKey,3600 * 24, dimJsonObj.toJSONString());
                }
            }
            else {
                System.out.println("维度数据没有找到：" + sql);
            }
        }
        //关闭Jedis
        if(jedis != null){
            jedis.close();
        }
        return dimJsonObj;
    }

    public static void deleteCached(String tableName, String id){
        try {
            String key = "dim:" + tableName.toLowerCase() + ":" +id;
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("缓存异常！");
        }
    }

    public static void main(String[] args) {
//        System.out.println(getDimInfoNoCache("DIM_USER_INFO",Tuple2.of("id", "13")));
        System.out.println(getDimInfoCache("DIM_BASE_TRADEMARK","14"));

    }
}
