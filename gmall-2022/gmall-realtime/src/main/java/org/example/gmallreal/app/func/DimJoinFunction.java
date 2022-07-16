package org.example.gmallreal.app.func;

import com.alibaba.fastjson.JSONObject;

//维度关联接口
public interface DimJoinFunction<T> {
    //需要一个提供key的方法，定义成抽象方法，便于后续在具体的问题中具体实现，传到具体的key
    String getKey(T obj);

    //定义一个抽象方法，将事实数据和维度数据进行关联，因为没有具体的维度数据和事实数据，没有办法直接进行关联，所以需要其子类实现这个方法
    void join(T obj, JSONObject dimJsonObj) throws Exception;
}
