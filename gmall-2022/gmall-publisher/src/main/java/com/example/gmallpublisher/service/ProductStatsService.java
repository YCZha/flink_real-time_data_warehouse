package com.example.gmallpublisher.service;


import com.example.gmallpublisher.bean.ProductStats;

import java.math.BigDecimal;
import java.util.List;

//商品统计service接口
public interface ProductStatsService {
    //获取某一天的交易总额
    BigDecimal getGMV(int date);

    //获取某一天 不同品牌的交易额，并按大小排序，拿到最大的limit数量的品牌相关信息
    List<ProductStats> getProductStatsByTrademark(int date, int limit);

    //获取某一天不同品类交易额
    List<ProductStats> getProductStatsByCategory3(int date, int limit);

    //统计某天不同 SPU 商品交易额排名
    public List<ProductStats> getProductStatsBySpu(int date, int limit);

}
