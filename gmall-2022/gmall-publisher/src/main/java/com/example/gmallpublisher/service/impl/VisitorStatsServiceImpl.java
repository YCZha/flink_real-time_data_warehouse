package com.example.gmallpublisher.service.impl;

import com.example.gmallpublisher.bean.VisitorStats;
import com.example.gmallpublisher.mapper.VisitorStatsMapper;
import com.example.gmallpublisher.service.VisitorStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class VisitorStatsServiceImpl implements VisitorStatsService {

    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewFlag(int date) {
        return visitorStatsMapper.getVisitorStatsByNewFlag(date);
    }

    @Override
    public List<VisitorStats> getVisitorStatsByHour(int date) {
        return visitorStatsMapper.getVisitorStatsByHour(date);
    }

    @Override
    public Long getPv(int date) {
        return visitorStatsMapper.getPv(date);
    }

    @Override
    public Long getUv(int date) {
        return visitorStatsMapper.getUv(date);
    }
}
