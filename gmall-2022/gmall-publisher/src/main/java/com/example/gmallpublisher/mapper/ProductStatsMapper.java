package com.example.gmallpublisher.mapper;


import com.example.gmallpublisher.bean.ProductStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

//商品主题统计的mapper接口
public interface ProductStatsMapper {
    //获取某一天的商品的交易额
    @Select("select sum(order_amount) from product_stats_2022 where toYYYYMMDD(stt)=#{date}")
    BigDecimal getGMV(int date);

    //select tm_id,tm_name,sum(order_amount) order_amount
    //                      from product_stats_2022
    //                      where toYYYYMMDD(stt)=20220622
    //                      group by tm_id,tm_name
    //                      having order_amount > 0
    //                      order by order_amount desc limit 5;

    //获取某一天 不同品牌的交易额，并按大小排序，拿到最大的limit数量的品牌相关信息
    //如果mybatis有多个参数，需要使用param注解来指定参数对应的名称
    @Select("select tm_id,tm_name,sum(order_amount) order_amount from product_stats_2022 " +
            "where toYYYYMMDD(stt)=#{date} group by tm_id,tm_name having order_amount > 0 " +
            "order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsByTrademark(@Param("date") int date, @Param("limit") int limit);

    //获取莫一天，不同品类的交易额
    @Select("select category3_id,category3_name,sum(order_amount) order_amount from product_stats_2022 " +
            "where toYYYYMMDD(stt)=#{date} group by category3_id,category3_name having order_amount > 0 " +
            "order by order_amount desc limit #{limit}")
    List<ProductStats> getProductStatsByCategory3(@Param("date") int date, @Param("limit") int limit);

    //统计某天不同 SPU 商品交易额排名
    @Select("select spu_id,spu_name,sum(order_amount) order_amount," +
            "sum(order_ct) order_ct from product_stats_2022 " +
            "where toYYYYMMDD(stt)=#{date} group by spu_id,spu_name " +
            "having order_amount>0 order by order_amount desc limit #{limit} ")
    List<ProductStats> getProductStatsBySpu(@Param("date") int date, @Param("limit") int limit);

}
