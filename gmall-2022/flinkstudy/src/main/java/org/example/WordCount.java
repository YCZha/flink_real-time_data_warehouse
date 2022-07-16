package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//批处理wordcount数据
public class WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //从文件中读取数据
        String inputPath = "D:\\Data\\flinkproj\\gmall-2022\\flinkstudy\\src\\main\\resources\\mockdata.txt";
        DataSet<String> dataSource = env.readTextFile(inputPath);

        //对数据集进行处理
        DataSet<Tuple2<String, Integer>> resultSet = dataSource.flatMap(new MyFlatMapper())
                .groupBy(0)
                .sum(1);// 按照第一位置的word分组
        resultSet.print();
    }
    //自定义嘞，实现FlatMapper接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //按空格分词
            String[] words = s.split(" ");
            for(String word : words){
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
