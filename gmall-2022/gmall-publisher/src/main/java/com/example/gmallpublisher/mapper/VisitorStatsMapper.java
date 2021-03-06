package com.example.gmallpublisher.mapper;

import com.example.gmallpublisher.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface VisitorStatsMapper {

    //新老访客流量统计
    @Select("select is_new,sum(uv_ct) uv_ct,sum(pv_ct) pv_ct," +
            "sum(sv_ct) sv_ct, sum(uj_ct) uj_ct,sum(dur_sum) dur_sum " +
            "from visitor_stats_2022 where toYYYYMMDD(stt)=#{date} group by is_new")
    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    //分时流量统计
    @Select("select sum(if(is_new='1', visitor_stats_2022.uv_ct,0)) new_uv,toHour(stt) hr," +
            "sum(visitor_stats_2022.uv_ct) uv_ct, sum(pv_ct) pv_ct, sum(uj_ct) uj_ct " +
            "from visitor_stats_2022 where toYYYYMMDD(stt)=#{date} group by toHour(stt)")
    List<VisitorStats> getVisitorStatsByHour(int date);

    @Select("select count(pv_ct) pv_ct from visitor_stats_2022 " +
            "where toYYYYMMDD(stt)=#{date} ")
    Long getPv(int date);

    @Select("select count(uv_ct) uv_ct from visitor_stats_2022 " +
            "where toYYYYMMDD(stt)=#{date} ")
    Long getUv(int date);
}
