package com.example.gmallpublisher.service.impl;

import com.example.gmallpublisher.bean.ProvinceStats;
import com.example.gmallpublisher.mapper.ProvinceStatsMapper;
import com.example.gmallpublisher.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {

    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        return provinceStatsMapper.getProvinceStats(date);
    }
}
