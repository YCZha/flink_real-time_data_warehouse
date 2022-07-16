package org.example;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//设置并行度，可以同时执行多个

        //1.1开启CK,并指定状态后端为FS。CK checkpoint 如果服务器崩溃，可以从checkpoint处启动
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink/ck"));
        env.enableCheckpointing(5000);//五秒钟做一次checkpoint
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);//设置checkpoint的模式为精确一次
        env.getCheckpointConfig().setCheckpointTimeout(10000L);//设置超时时间十秒
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);//表示同时可以打开多少个ck
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000);//两次做checkpoint中间的间隔时间

//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart()); //设置重启策略，最大重启次数，重启间隔

        //2.通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("980411")
                .databaseList("gmall-flink")
                .tableList("gmall-flink.base_trademark") //如果不指定，则消费库中所有的表，如果指定需要写db.tablename的格式
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())//启动的方式，先加锁记录监控数据库的快照，再读binlog
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        //3.打印数据
        streamSource.print();
        //4.启动任务
        env.execute("FlinkCDC");
    }
}
