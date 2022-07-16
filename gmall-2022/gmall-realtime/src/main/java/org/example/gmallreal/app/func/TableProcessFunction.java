package org.example.gmallreal.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.protocol.types.Field;
import org.example.gmallreal.bean.TableProcess;
import org.example.gmallreal.common.GmallConfig;
import org.example.gmallreal.utils.MyKafkaUtil;
import org.example.gmallreal.utils.MySQLUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;


//mysql配置表处理函数
public class TableProcessFunction extends ProcessFunction<JSONObject,JSONObject> {
    //因为要将维度数据从侧输出流输出，所以定义侧输出流标记
    private OutputTag<JSONObject> outputTag;

    //用于在内存中防止配置表信息，<表名：操作，tableProcess>
    private Map<String, TableProcess> tableProcessMap = new HashMap<>();

    //用于在内存中存放以及在hbase中已经建过的表。因为如果不存，每次phoneix都需要执行CREATE TABLE IF NOT EXSIT语句
    private  Set<String> existTables = new HashSet<>();

    //声明phoenix的连接对象
    Connection conn = null;

    //实例化函数对象，将侧输出流标记初始化
    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    //算子生命周期开始时调用
    //从mysql中读取配置文件，并开启定时任务，定时从mysql中读取配置文件，并配段是否更改，检查Hbase中表是否需要更改，需要则增加
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化phoenix连接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //初始化配置表信息，读取到内存中去。
        refreshMeta();
        //因为配置表的数据会发生变化，需要开启一个定时任务，每隔一段时间，就从配置表中查询数据，并检查建Hbase表，对内存tableMap进行更新
        Timer timer = new Timer();
        //从现在起过delay毫秒后，每隔period时间执行一次
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                refreshMeta();
            }
        },5000,5000);
    }

    private void refreshMeta() {
        //从mysql配置表中查询配置信息
        System.out.println("查询数据表信息");
        List<TableProcess> tableProcessList = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        //对查询的结果进行遍历，并存放到tableMap中,将数据保存到内存中。
        for (TableProcess tableProcess : tableProcessList) {
            //============获取信息================
            String sourceTable = tableProcess.getSourceTable(); //获取源表表名
            String operateType = tableProcess.getOperateType(); //获取操作类型
            String sinkType = tableProcess.getSinkType(); //输出类型 hbase/kafka
            String sinkTable = tableProcess.getSinkTable(); //输出目的地表名或者主题名
            String sinkColumns = tableProcess.getSinkColumns(); //输出字段
            String sinkPk = tableProcess.getSinkPk(); //输出主键
            String sinkExtend = tableProcess.getSinkExtend(); //建表拓展语句
            //将配置表中查询到的配置信息保存到map集合中
            //拼接保存配置的key
            String key = sourceTable + ":" + operateType;
            //================存放到map里=================
            tableProcessMap.put(key, tableProcess);
            //================检查，并建hbase表=========
            //如果当前配置是维度配置，需要向Hbase中保存数据，需要判断Hbase中是否存在该表，不存在则需要创建。
            if(TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)){
                boolean notExist = existTables.add(sourceTable); //不存在返回true,因为不存在就能插入成功
                //如果内存中set不存在这张表，则需要在hbase中创建这张表
                if(notExist){
                    //检查Phoneix中是否存在该表，因为有可能存在，只不过因为缓存清空，内存中set没了,所以需要create table if not exist。
                    checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
                }
            }

        }
        //如果没有从数据库的配置表中读取到数据
        if(tableProcessMap == null || tableProcessMap.size() == 0){
            throw new RuntimeException("没有从数据库的配置表中读取到数据");
        }

    }

    //检查并建表
    private void checkTable(String tableName, String fields, String pk, String ext) {
        //如果配置表中没有配置主键，需要给一个默认主键的值
        if(pk == null){
            pk = "id";
        }
        //如果配置表中没有配置建表扩展，需要给一个默认建表扩展的值
        if(ext == null){
            ext = "";
        }
        //拼接建表语句
        StringBuilder createSql = new StringBuilder("create table if not exists " +
                GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        String[] fieldsArr = fields.split(",");
        for (int i = 0; i < fieldsArr.length; i++) {
            String field = fieldsArr[i];
            if(pk.equals(field)){
                createSql.append(field).append(" varchar primary key ");
            }
            else{
                createSql.append("info.").append(field).append(" varchar ");
            }
            if(i < fieldsArr.length - 1){
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);

        System.out.println("创建phoenix表语句："+ createSql);
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(createSql.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException("Phoenix建表失败");
                }
            }
        }

    }

    //处理单个流数据的方法，主要任务是对当前进来的元素做一个分流处理
    @Override
    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        //获取表名
        String table = jsonObject.getString("table");
        //获取操作类型
        String type = jsonObject.getString("type");
        //注意：问题修复，因为maxwell同步历史数据，会将insert转化为bootstrap-insert
        if("bootstrap-insert".equals(type)){
            type = "insert";
            jsonObject.put("type",type);
        }
        //从内存中的配置map中来获取当前key所对应的配置信息
        if(tableProcessMap != null && tableProcessMap.size() > 0){
            //根据表名和操作类型来拼接key
            String key = table +":"+type;
            TableProcess tableProcess = tableProcessMap.get(key);
            //如果获取到该元素的配置信息，则进行相关操作
            if(tableProcess != null){
                String sinkTable = tableProcess.getSinkTable(); //指该记录应该发往kafka还是hbase
                jsonObject.put("sink_table",sinkTable); //给json中加入一条数据，用于标记发往何处
                //如果制定了sinkCloumn，需要对保留的字段进行过滤处理
                String sinkColumns = tableProcess.getSinkColumns();
                if(sinkColumns !=null && sinkColumns.length() > 0){
                    //对数据进行过滤，删除不必要的数据
                    filterCloumn(jsonObject.getJSONObject("data"),sinkColumns);
                }

            }
            else{
                System.out.println("No this key:" + key + " in MYSQL");
            }
            //根据sinktype，将数据输出到不同的流
            if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)){
                //如果type是hbase,则从侧输出流输出
                context.output(outputTag,jsonObject);
            }
            else if(tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果是kafka，则从主流输出
                collector.collect(jsonObject);
            }
        }
    }

    //对data中的数据进行一个过滤，因为表中的有些字段是不需要的，比如手机品牌的log图，留下哪些内容配置表中已经标出
    private void filterCloumn(JSONObject data, String sinkColumns) {
        //取出配置表中的输出列的各列
        String[] clos = sinkColumns.split(",");
        //转化成List
        List<String> cloumnList = Arrays.asList(clos);
        //将json对象转化为KV的entrySet，每一个entry是一个KV
        Set<Map.Entry<String, Object>> entrySet = data.entrySet();
        //获取迭代器
        Iterator<Map.Entry<String, Object>> it = entrySet.iterator();
        for(;it.hasNext();){
            Map.Entry<String, Object> entry = it.next();
            if(!cloumnList.contains(entry.getKey())){
                //使用迭代器删除，不会出错
                it.remove();
            }
        }
    }
}
