package org.example.gmallreal.bean;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

//商品统计实体类
@Data
@Builder //帮我们使用构造者模式自动构建build类
public class ProductStats {
    //维度
    String stt;//窗口起始时间
    String edt; //窗口结束时间
    Long sku_id; //sku 编号
    String sku_name;//sku 名称
    BigDecimal sku_price; //sku 单价
    Long spu_id; //spu 编号
    String spu_name;//spu 名称
    Long tm_id; //品牌编号
    String tm_name;//品牌名称
    Long category3_id;//品类编号
    String category3_name;//品类名称
    //度量
    @Builder.Default
    Long display_ct = 0L; //曝光数
    @Builder.Default
    Long click_ct = 0L; //点击数
    @Builder.Default
    Long favor_ct = 0L; //收藏数
    @Builder.Default
    Long cart_ct = 0L; //添加购物车数
    @Builder.Default
    Long order_sku_num = 0L; //下单商品个数
    @Builder.Default
    BigDecimal order_amount = BigDecimal.ZERO; //下单商品金额
    @Builder.Default
    Long order_ct = 0L; //订单数
    @Builder.Default //支付金额
    BigDecimal payment_amount = BigDecimal.ZERO;
    @Builder.Default
    Long paid_order_ct = 0L; //支付订单数
    @Builder.Default
    Long refund_order_ct = 0L; //退款订单数
    @Builder.Default
    BigDecimal refund_amount = BigDecimal.ZERO;
    @Builder.Default
    Long comment_ct = 0L;//评论数
    @Builder.Default
    Long good_comment_ct = 0L; //好评数

    //用于统计 不需要往数据库保存，所以加上注解TransientSink
    //主要是为了去重，因为多个订单明细对应一个订单，如果只是通过统计订单id来判断其订单数，支付订单数或者退款订单数的话，就会有重复的数据
    @Builder.Default
    @TransientSink
    Set orderIdSet = new HashSet(); //用于统计订单数
    @Builder.Default
    @TransientSink
    Set paidOrderIdSet = new HashSet(); //用于统计支付订单数
    @Builder.Default
    @TransientSink
    Set refundOrderIdSet = new HashSet();//用于退款支付订单数
    Long ts; //统计时间戳
}
