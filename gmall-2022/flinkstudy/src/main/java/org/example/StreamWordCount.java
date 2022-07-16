package org.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        //从文件中读取数据
//        String inputPath = "D:\\Data\\flinkproj\\gmall-2022\\flinkstudy\\src\\main\\resources\\mockdata.txt";
//        DataStream<String> dataSource = env.readTextFile(inputPath);


        //从程序启动参数中提取配置项
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //在集群中，不能从文件中读入，这样是有界数据，应该用消息中间件，如kafka
        DataStream<String> dataSource = env.socketTextStream(host,port);

        //基于数据流进行转换操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataSource.flatMap(new WordCount.MyFlatMapper())
                .keyBy(0)
                .sum(1);
        resultStream.print();


        //执行任务
        env.execute();
    }
}
