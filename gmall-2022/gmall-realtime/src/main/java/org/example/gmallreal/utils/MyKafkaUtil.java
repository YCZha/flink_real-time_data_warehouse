package org.example.gmallreal.utils;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

//自定义的操作kafka的工具类
public class MyKafkaUtil {

    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    private static  String DEFAULT_TOPIC = "DEFAULT_DATA";
    //封装一个方法，获取FlinkKafkaConsumer：主题和消费者组
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId){
        //Kafka连接的属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), props);
    }

    //封装FLinkKafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        //序列化是flink对象对kafka之间序列化方式,是序列化一个String的，不能序列化json
        return new FlinkKafkaProducer<String>(kafkaServer, topic, new SimpleStringSchema());
    }

    //该方法相比于上方法，可以指定topic和根据传入对象类型指定序列化方式
    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        //设置属性
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 15*60*1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,props,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //拼接 Kafka 相关属性到 DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"'," +
                " 'properties.bootstrap.servers' = '"+ kafkaServer +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset' ";
        return ddl;
    }

}
