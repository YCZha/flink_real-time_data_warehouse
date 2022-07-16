package com.example.gmallpublisher.service;

import com.example.gmallpublisher.bean.VisitorStats;

import java.util.List;

//访客统计
public interface VisitorStatsService {

    //得到新老访客的相关数据，包括是否是新老访客的标签
    List<VisitorStats> getVisitorStatsByNewFlag(int date);

    //得到每个小时内的新访客数，独立访客数，pv等
    List<VisitorStats> getVisitorStatsByHour(int date);

    //得到当天所有pv
    Long getPv(int date);

    //得到当天的所有uv
    Long getUv(int date);

}
