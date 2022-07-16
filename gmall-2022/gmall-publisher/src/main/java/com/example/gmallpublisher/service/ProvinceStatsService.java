package com.example.gmallpublisher.service;

import com.example.gmallpublisher.bean.ProvinceStats;

import java.util.List;

public interface ProvinceStatsService {
    //地区维度统计，获得某一天各个地区的订单总量
    List<ProvinceStats> getProvinceStats(int date);
}
