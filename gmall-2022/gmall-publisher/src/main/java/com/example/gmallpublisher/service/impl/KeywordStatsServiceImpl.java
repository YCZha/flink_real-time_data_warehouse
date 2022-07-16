package com.example.gmallpublisher.service.impl;

import com.example.gmallpublisher.bean.KeywordStats;
import com.example.gmallpublisher.mapper.KeywordStatsMapper;
import com.example.gmallpublisher.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {

    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        return keywordStatsMapper.getKeywordStats(date,limit);
    }
}
