package com.zyc.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducer {
    public static void main(String[] args) throws InterruptedException {
        //添加配置
        Properties properties = new Properties();
        //连接kafka集群
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092");
        //序列化
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //可以指定分区配置
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.zyc.kafka.MyPratitions");

        //提高生产者吞吐量
        //配置缓冲区大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,33554432);
        //配置批次大小
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,  16384);
        //配置linger ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //配置压缩技术
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        //数据的可靠性问题
        //设置ack,当开启事务时，必须要将ack设置为all
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //设置retry次数
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        //设置事务id
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction_id_01");


        //建立kafka生产者
        KafkaProducer producer = new KafkaProducer<String, String>(properties);

        //添加事务  保证只执行一次
        //初始化事务
        producer.initTransactions();
        //开启事务
        producer.beginTransaction();
        try{
            //发送消息
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord("first", "zyc" + i), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            System.out.println("主题："+recordMetadata.topic()+"partition:"+recordMetadata.partition());
                        }
                        else{
                            e.printStackTrace();
                        }
                    }
                });
                Thread.sleep(2);
            }
            //提交事务
            producer.commitTransaction();
        }catch (Exception e){
            //终止事务
            producer.abortTransaction();
        } finally{
            //关闭连接
            producer.close();
        }
    }
}
