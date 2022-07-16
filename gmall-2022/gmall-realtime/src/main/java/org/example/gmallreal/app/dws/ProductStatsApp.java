package org.example.gmallreal.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.gmallreal.app.func.DimAsyncFunction;
import org.example.gmallreal.common.GmallConstant;
import org.example.gmallreal.bean.OrderWide;
import org.example.gmallreal.bean.PaymentWide;
import org.example.gmallreal.bean.ProductStats;
import org.example.gmallreal.utils.ClickHouseUtil;
import org.example.gmallreal.utils.DataTimeUtil;
import org.example.gmallreal.utils.MyKafkaUtil;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import static javax.swing.UIManager.getString;

//商品主题统计应用
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1. 基本环境准备
        //配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        env.setParallelism(3);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/checkpoint/productstatsapp"));
        env.setRestartStrategy(RestartStrategies.noRestart());
        System.setProperty("HADOOP_USER_NAME", "zyc");

        //TODO 2.从kafka中读取数据
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log"; //页面日志
        String orderWideSourceTopic = "dwm_order_wide"; //订单宽表
        String paymentWideSourceTopic = "dwm_payment_wide"; //支付宽表
        String cartInfoSourceTopic = "dwd_cart_info"; //购物车表
        String favorInfoSourceTopic = "dwd_favor_info";  //收藏表
        String refundInfoSourceTopic = "dwd_order_refund_info";  //退款表
        String commentInfoSourceTopic = "dwd_comment_info";  //评价表

        String productStatsSinkTopic = "dws_product_stats";

        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));//从页面日志中获取点击和曝光
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));//从订单宽表中获得下单数据
        DataStreamSource<String> paymentWideDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));//从支付宽表中获取支付数据
        DataStreamSource<String> cartInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));//从购物车表中获取加购数据
        DataStreamSource<String> favorInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));//收藏表中获取收藏数据
        DataStreamSource<String> refundInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));//从退款表中获得退款数据
        DataStreamSource<String> commentInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));//从评价表中获取评价数据


        paymentWideDS.print("payment>>>>>>>>>>>>>>>>");
        //TODO 3.将不同的流数据转化为统一的数据格式——ProductStats
        //3.1对点击、曝光数据进行转换
        SingleOutputStreamOperator<ProductStats> productClickAndDisPlaysDS = pageViewDS.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, ProductStats>.Context context, Collector<ProductStats> collector) throws Exception {
                        //将jsonStr->jsonobj
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");

                        if (pageId == null) {
                            System.out.println("页面数据为空：" + jsonObj);
                        }
                        //获取操作时间
                        Long ts = jsonObj.getLong("ts");
                        if ("good_detail".equals(pageId)) { //进入商品详情页，表示点击了
                            //获取被点击商品的id
                            Long skuId = pageJsonObj.getLong("item");
                            //封装一次点击操作
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                            //向下游输出
                            collector.collect(productStats);
                        }
                        JSONArray displays = jsonObj.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                //获取曝光数据
                                JSONObject displaysJsonObj = displays.getJSONObject(i);
                                //判断是否曝光的是某一个商品
                                if ("sku_id".equals(displaysJsonObj.getString("item_type"))) {
                                    //获取商品id
                                    Long skuId = displaysJsonObj.getLong("item");
                                    //封装曝光商品对象
                                    ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                    //向下游输出
                                    collector.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );
        //3.2 对订单宽表进行转换
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        //将jsonStr转换为订单宽表对象
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                        String create_time = orderWide.getCreate_time();
                        Long ts = DataTimeUtil.toTS(create_time); //以为要开窗，所以时间要统一转换为毫秒数
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .ts(ts)
                                //用于计算订单数，因为多个订单明细对应一个订单，且订单中的商品数量不一定，所以先把订单号记录下来
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .build();

                        return productStats;
                    }
                }
        );
        //3.3 转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long ts = DataTimeUtil.toTS(jsonObj.getString("create_time")); //以为要开窗，所以时间要统一转换为毫秒数
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(ts)
                                .build();
                        return productStats;
                    }
                }
        );
        //3.4 转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long ts = DataTimeUtil.toTS(jsonObj.getString("create_time")); //以为要开窗，所以时间要统一转换为毫秒数
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .cart_ct(1L)
                                .ts(ts)
                                .build();
                        return productStats;
                    }
                }
        );
        //3.5 转换支付流数据
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                        Long ts = DataTimeUtil.toTS(paymentWide.getPayment_create_time());
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                                .ts(ts)
                                .build();
                        return productStats;
                    }
                }
        );
        //3.6 转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long ts = DataTimeUtil.toTS(jsonObj.getString("create_time")); //以为要开窗，所以时间要统一转换为毫秒数
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .refund_amount(jsonObj.getBigDecimal("refund_amount"))
                                .refundOrderIdSet(new HashSet(Collections.singleton(jsonObj.getLong("order_id"))))
                                .ts(ts)
                                .build();
                        return productStats;
                    }
                }
        );
        //3.7 转换评价流数据
        SingleOutputStreamOperator<ProductStats> commentStatsDS = commentInfoDS.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long ts = DataTimeUtil.toTS(jsonObj.getString("create_time")); //以为要开窗，所以时间要统一转换为毫秒数
                        Long good_ct = GmallConstant.APPRAISE_GOOD.equals(jsonObj.getString("appraise")) ? 1L : 0L;
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .comment_ct(1L)
                                .good_comment_ct(good_ct)
                                .ts(ts)
                                .build();
                        return productStats;
                    }
                }
        );

        //TODO 4. 将转换后的流进行合并
        DataStream<ProductStats> unionDS = productClickAndDisPlaysDS.union(
                orderWideStatsDS,
                favorStatsDS,
                cartStatsDS,
                paymentStatsDS,
                refundStatsDS,
                commentStatsDS
        );

        //TODO 5. 设置watermark，提取事件时间字段
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long l) {
                                        return productStats.getTs();
                                    }
                                }
                        )
        );

        //TODO 6. 按照维度对数据进行分组
        KeyedStream<ProductStats, Long> productStatsKeyedDS = productStatsWithWatermarkDS.keyBy(
                new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats productStats) throws Exception {
                        return productStats.getSku_id();
                    }
                }
        );

        //TODO 7. 对分组之后的数据进行开窗，开十秒的滚动窗口
        WindowedStream<ProductStats, Long, TimeWindow> productStatsWithWindowDS = productStatsKeyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //TODO 8. 对窗口中的元素进行聚合操作
        SingleOutputStreamOperator<ProductStats> reduceDS = productStatsWithWindowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats productStats1, ProductStats productStats2) throws Exception {
                        productStats1.setDisplay_ct(productStats1.getDisplay_ct() + productStats2.getDisplay_ct());
                        productStats1.setClick_ct(productStats1.getClick_ct() + productStats2.getClick_ct());
                        productStats1.setCart_ct(productStats1.getCart_ct() + productStats2.getCart_ct());
                        productStats1.setFavor_ct(productStats1.getFavor_ct() + productStats2.getFavor_ct());
                        productStats1.setOrder_amount(productStats1.getOrder_amount().add(productStats2.getOrder_amount()));
                        productStats1.getOrderIdSet().addAll(productStats2.getOrderIdSet());
                        productStats1.setOrder_ct(productStats1.getOrderIdSet().size() + 0L);
                        productStats1.setOrder_sku_num(productStats1.getOrder_sku_num() + productStats2.getOrder_sku_num());

                        productStats1.getRefundOrderIdSet().addAll(productStats2.getRefundOrderIdSet());
                        productStats1.setRefund_order_ct(productStats1.getRefundOrderIdSet().size() + 0L);
                        productStats1.setRefund_amount(productStats1.getRefund_amount().add(productStats2.getRefund_amount()));

                        productStats1.getPaidOrderIdSet().addAll(productStats2.getPaidOrderIdSet());
                        productStats1.setPaid_order_ct(productStats1.getPaidOrderIdSet().size() + 0L);
                        productStats1.setPayment_amount(productStats1.getPayment_amount().add(productStats2.getPayment_amount()));

                        productStats1.setComment_ct(productStats1.getComment_ct() + productStats2.getComment_ct());
                        productStats1.setGood_comment_ct(productStats1.getGood_comment_ct() + productStats2.getGood_comment_ct());

                        return productStats1;
                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {

                    @Override
                    public void process(Long aLong, ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>.Context context, Iterable<ProductStats> elements, Collector<ProductStats> collector) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats productStats : elements) {
                            productStats.setStt(sdf.format(new Date(context.window().getStart())));
                            productStats.setEdt(sdf.format(new Date(context.window().getEnd())));
                            productStats.setTs(new Date().getTime());
                            collector.collect(productStats);
                        }
                    }
                }

        );

        //TODO 9. 补充维度信息
        //9.1 关联商品维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimJsonObj) throws Exception {
                        productStats.setSku_name(dimJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimJsonObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimJsonObj.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //9.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSPUDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats obj, JSONObject dimJsonObj) throws Exception {
                        obj.setSpu_name(dimJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //9.3 关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(
                productStatsWithSPUDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats obj, JSONObject dimJsonObj) throws Exception {
                        obj.setTm_name(dimJsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //9.4 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
                productStatsWithTMDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats obj, JSONObject dimJsonObj) throws Exception {
                        obj.setCategory3_name(dimJsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        productStatsWithCategoryDS.print("reduce>>>>>>>>>>>>>");


        productStatsWithCategoryDS.addSink(
                ClickHouseUtil
                        .<ProductStats>getJdbcSink("insert into product_stats_2022 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        productStatsWithCategoryDS.map(
                productStats->JSON.toJSONString(productStats,new SerializeConfig(true))
        ).addSink(MyKafkaUtil.getKafkaSink(productStatsSinkTopic));

        env.execute();
    }
}
