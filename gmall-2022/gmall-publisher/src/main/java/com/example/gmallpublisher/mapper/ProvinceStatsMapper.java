package com.example.gmallpublisher.mapper;

import com.example.gmallpublisher.bean.ProvinceStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

//地区维度统计Mapper
public interface ProvinceStatsMapper {
    @Select("select province_name,sum(order_amount) order_amount from province_stats_2022 " +
            "where toYYYYMMDD(stt)=#{date} group by province_id,province_name")
    List<ProvinceStats> getProvinceStats(int date);
}
