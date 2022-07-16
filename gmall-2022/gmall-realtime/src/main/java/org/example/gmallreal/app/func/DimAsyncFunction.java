package org.example.gmallreal.app.func;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.example.gmallreal.utils.DimUtil;
import org.example.gmallreal.utils.ThreadPoolUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 模板方法设计模式：
 *      在父类中之定义方法的声明，让整个流程跑通
 *      具体的实现延迟到子类中实现（子类重写该抽象方法）
 * @param <T>
 */
//自定义维度异步查询的函数，将类定义成抽象类，因为声明了一个getKey的抽象方法，便于在具体情况下拿到具体的key。
//修改为定义了一个接口，将需要根据具体内容实现的方法放到接口中，所以后续需要实现该方法，这样相比于前者不容易乱
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T>{
    //线程池对象的父接口声明（多态）
    private ExecutorService executorService = null;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        executorService = ThreadPoolUtil.getInstance();
        System.out.println("初始化线程池对象");
    }

    /**
     *
     * @param obj 流中的事实数据
     * @param resultFuture 异步处理之后的返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(
                new Runnable() {
                    //发送异步请求
                    @Override
                    public void run() {
                        try {
                            long start = System.currentTimeMillis();
                            //从事实数据中获取key，具体通过实现该类的子类来进行传递，多态
                            String key = getKey(obj);
                            //根据维度的主键到维度表中进行查询
                            JSONObject dimInfoJsonObj = DimUtil.getDimInfoCache(tableName, key);
                            System.out.println("维度数据Json格式："+ dimInfoJsonObj);
                            if(dimInfoJsonObj != null){
                                //维度数据关联 流中的事实数据和查询出来的维度数据进行关联
                                join(obj,dimInfoJsonObj);

                            }
                            System.out.println("维度关联后的对象："+ obj);
                            long end = System.currentTimeMillis();
                            System.out.println("异步任务:"+ key +"维度时间耗时：" + (end - start) + "ms");
                            //将关联后的数据向下传递
                            resultFuture.complete(Arrays.asList(obj)); //将关联后的事实数据输出出去
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(tableName + "维度异步查询失败");
                        }
                    }
                }
        );
    }
//    //需要一个提供key的方法，定义成抽象方法，便于后续在具体的问题中具体实现，传到具体的key
//    public abstract String getKey(T obj);
//
//    //定义一个抽象方法，将事实数据和维度数据进行关联，因为没有具体的维度数据和事实数据，没有办法直接进行关联，所以需要其子类实现这个方法
//    public abstract void join(T obj, JSONObject dimJsonObj);
}
