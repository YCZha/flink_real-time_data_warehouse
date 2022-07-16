package com.example.gmallpublisher.service;

import com.example.gmallpublisher.bean.KeywordStats;

import java.util.List;

public interface KeywordStatsService {

    List<KeywordStats> getKeywordStats(int date, int limit);
}
