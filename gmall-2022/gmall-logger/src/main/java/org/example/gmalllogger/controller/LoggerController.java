package org.example.gmalllogger.controller;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.protocol.types.Field;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate; //自行建立一个kafka生产者

    @RequestMapping("test")
    public String test1(){
        System.out.println("successs");
        return "success";
    }
    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") int age){
        System.out.println(name+":"+age);
        return "success";
    }

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
        //数据落盘
//        System.out.println(jsonStr);
        //找到logback配置文件，打印到相关地点
        log.info(jsonStr);
        //数据写入Kafka
        kafkaTemplate.send("ods_base_log", jsonStr);
        return "success";
    }
}
