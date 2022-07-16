package com.zyc.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

//自定义分区器，可以根据需求将数据放到指定的分区

public class MyPratitions implements Partitioner {
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        String value = o1.toString();
        int partition;
        if(value.contains("zyc")){
            partition = 0;
        }
        else {
            partition = 1;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
