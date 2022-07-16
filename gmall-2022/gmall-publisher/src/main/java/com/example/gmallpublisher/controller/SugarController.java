package com.example.gmallpublisher.controller;

//大屏展示控制层
//主要职责，接收客户端请求（request），对请求进行处理，并给客户端响应(response)

import com.example.gmallpublisher.bean.KeywordStats;
import com.example.gmallpublisher.bean.ProductStats;
import com.example.gmallpublisher.bean.ProvinceStats;
import com.example.gmallpublisher.bean.VisitorStats;
import com.example.gmallpublisher.service.KeywordStatsService;
import com.example.gmallpublisher.service.ProductStatsService;
import com.example.gmallpublisher.service.ProvinceStatsService;
import com.example.gmallpublisher.service.VisitorStatsService;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@RestController
@RequestMapping("/api/sugar")
public class SugarController {
    @Autowired
    ProductStatsService productStatsService;

    @Autowired
    ProvinceStatsService provinceStatsService;

    @Autowired
    VisitorStatsService visitorStatsService;

    @Autowired
    KeywordStatsService keywordStatsService;

    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date",defaultValue = "0") Integer date) {
        if(date==0){
            date=now();
        }
        BigDecimal gmv = productStatsService.getGMV(date);
        String json = "{ \"status\": 0, \"data\":" + gmv + "}";
        return json;
    }
    private int now(){
        String yyyyMMdd = DateFormatUtils.format(new Date(), "yyyyMMdd");
        return Integer.valueOf(yyyyMMdd);
    }


    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit){
        if(date == 0){
            date = now();
        }

        List<ProductStats> productStatsByTrademark = productStatsService.getProductStatsByTrademark(date, limit);

        List<String> trademarkNameList = new ArrayList<>();
        List<BigDecimal> amountList = new ArrayList<>();

        //拼接json字符串
        for (ProductStats productStats : productStatsByTrademark) {
            trademarkNameList.add(productStats.getTm_name());
            amountList.add(productStats.getOrder_amount());
        }

        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\"" + StringUtils.join(trademarkNameList, "\",\"") + "\"],\"series\":[" +
                "{\"data\":[" + StringUtils.join(amountList, ",") + "]}]}}";

        return json;
    }

    @RequestMapping("/category3")
    public String getProductStatsGroupByCategory3(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "4") Integer limit) {
        if (date == 0) {
            date = now();
        }
        List<ProductStats> statsList
                = productStatsService.getProductStatsByCategory3(date, limit);
        StringBuilder dataJson = new StringBuilder("{ \"status\": 0, \"data\": [");
        int i = 0;
        for (ProductStats productStats : statsList) {
            if (i++ > 0) {
                dataJson.append(",");
            }
            dataJson.append("{\"name\":\"")
                    .append(productStats.getCategory3_name()).append("\",");
            dataJson.append("\"value\":")
                    .append(productStats.getOrder_amount()).append("}");
        }
        dataJson.append("]}");
        return dataJson.toString();
    }

    @RequestMapping("/spu")
    public String getProductStatsGroupBySpu(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
        if (date == 0) date = now();
        List<ProductStats> statsList
                = productStatsService.getProductStatsBySpu(date, limit);
        //设置表头
        StringBuilder jsonBuilder =
                new StringBuilder(" " +
                        "{\"status\":0,\"data\":{\"columns\":[" +
                        "{\"name\":\"商品名称\",\"id\":\"spu_name\"}," +
                        "{\"name\":\"交易额\",\"id\":\"order_amount\"}," +
                        "{\"name\":\"订单数\",\"id\":\"order_ct\"}]," +
                        "\"rows\":[");
        //循环拼接表体
        for (int i = 0; i < statsList.size(); i++) {
            ProductStats productStats = statsList.get(i);

            if (i >= 1) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("{\"spu_name\":\"" + productStats.getSpu_name() + "\"," +
                    "\"order_amount\":" + productStats.getOrder_amount() + "," +
                    "\"order_ct\":" + productStats.getOrder_ct() + "}");
        }
        jsonBuilder.append("]}}");
        return jsonBuilder.toString();
    }

    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date){
        if(date == 0){
            date = now();
        }
        StringBuilder jsonBuilder = new StringBuilder("{\"status\":0,\"data\":{\"mapData\":[");
        List<ProvinceStats> provinceStats = provinceStatsService.getProvinceStats(date);
        if (provinceStats.size() == 0) {
            jsonBuilder.append( "{\"name\":\"北京\",\"value\":0.00}");
        }
        for (int i = 0; i < provinceStats.size(); i++) {
            if (i >= 1) {
                jsonBuilder.append(",");
            }
            ProvinceStats provinceStat = provinceStats.get(i);
            jsonBuilder.append("{\"name\":\"" + provinceStat.getProvince_name() +
                    "\",\"value\":" + provinceStat.getOrder_amount() + " }");
        }
        jsonBuilder.append("]}}");
        return jsonBuilder.toString();
    }

    @RequestMapping("/visitor")
    public String getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0")
                                                   Integer date) {
        if (date == 0) date = now();
        List<VisitorStats> visitorStatsByNewFlag =
                visitorStatsService.getVisitorStatsByNewFlag(date);
        VisitorStats newVisitorStats = new VisitorStats();
        VisitorStats oldVisitorStats = new VisitorStats();
        //循环把数据赋给新访客统计对象和老访客统计对象
        for (VisitorStats visitorStats : visitorStatsByNewFlag) {
            if (visitorStats.getIs_new().equals("1")) {
                newVisitorStats = visitorStats;
            } else {
                oldVisitorStats = visitorStats;
            }
        }
        //把数据拼接入字符串
        String json = "{\"status\":0,\"data\":{\"combineNum\":1,\"columns\":" +
                "[{\"name\":\"类别\",\"id\":\"type\"}," +
                "{\"name\":\"新用户\",\"id\":\"new\"}," +
                "{\"name\":\"老用户\",\"id\":\"old\"}]," +
                "\"rows\":" +
                "[{\"type\":\"用户数(人)\"," +
                "\"new\": " + newVisitorStats.getUv_ct() + "," +
                "\"old\":" + oldVisitorStats.getUv_ct() + "}," +
                "{\"type\":\"总访问页面(次)\"," +
                "\"new\":" + newVisitorStats.getPv_ct() + "," +
                "\"old\":" + oldVisitorStats.getPv_ct() + "}," +
                "{\"type\":\"跳出率(%)\"," +
                "\"new\":" + newVisitorStats.getUjRate() + "," +
                "\"old\":" + oldVisitorStats.getUjRate() + "}," +
                "{\"type\":\"平均在线时长(秒)\"," +
                "\"new\":" + newVisitorStats.getDurPerSv() + "," +
                "\"old\":" + oldVisitorStats.getDurPerSv() + "}," +
                "{\"type\":\"平均访问页面数(人次)\"," +
                "\"new\":" + newVisitorStats.getPvPerSv() + "," +
                "\"old\":" + oldVisitorStats.getPvPerSv()
                + "}]}}";
        return json;
    }

    @RequestMapping("/hr")
    public String getMidStatsGroupbyHourNewFlag(@RequestParam(value = "date",defaultValue = "0") Integer date ) {
        if(date==0) date=now();
        List<VisitorStats> visitorStatsHrList
                = visitorStatsService.getVisitorStatsByHour(date);
        //构建 24 位数组
        VisitorStats[] visitorStatsArr=new VisitorStats[24];
        //把对应小时的位置赋值
        for (VisitorStats visitorStats : visitorStatsHrList) {
            visitorStatsArr[visitorStats.getHr()] =visitorStats ;
        }
        List<String> hrList=new ArrayList<>();
        List<Long> uvList=new ArrayList<>();
        List<Long> pvList=new ArrayList<>();
        List<Long> newMidList=new ArrayList<>();
        //循环出固定的 0-23 个小时 从结果 map 中查询对应的值
        for (int hr = 0; hr <=23 ; hr++) {
            VisitorStats visitorStats = visitorStatsArr[hr];
            if (visitorStats!=null){
                uvList.add(visitorStats.getUv_ct()) ;
                pvList.add( visitorStats.getPv_ct());
                newMidList.add( visitorStats.getNew_uv());
            }else{ //该小时没有流量补零
                uvList.add(0L) ;
                pvList.add( 0L);
                newMidList.add( 0L);
            }
            //小时数不足两位补零
            hrList.add(String.format("%02d", hr));
        }
        //拼接字符串
        String json = "{\"status\":0,\"data\":{" + "\"categories\":" +
                "[\""+StringUtils.join(hrList,"\",\"")+ "\"],\"series\":[" +
                "{\"name\":\"uv\",\"data\":["+ StringUtils.join(uvList,",") +"]}," +
                "{\"name\":\"pv\",\"data\":["+ StringUtils.join(pvList,",") +"]}," +
                "{\"name\":\"新用户\",\"data\":["+ StringUtils.join(newMidList,",") +"]}]}}";
        return json;
    }

    @RequestMapping("/keyword")
    public String getKeywordStats(@RequestParam(value = "date",defaultValue = "0") Integer date,
                                  @RequestParam(value = "limit",defaultValue = "20") int
                                          limit){
        if(date==0){
            date=now();
        }
        //查询数据
        List<KeywordStats> keywordStatsList
                = keywordStatsService.getKeywordStats(date, limit);
        StringBuilder jsonSb=new StringBuilder( "{\"status\":0,\"msg\":\"\",\"data\":[" );
        //循环拼接字符串
        for (int i = 0; i < keywordStatsList.size(); i++) {
            KeywordStats keywordStats = keywordStatsList.get(i);
            if(i>=1){
                jsonSb.append(",");
            }
            jsonSb.append( "{\"name\":\"" + keywordStats.getKeyword() + "\"," +
                    "\"value\":"+keywordStats.getCt()+"}");
        }
        jsonSb.append( "]}");
        return jsonSb.toString();
    }

}
