package org.example.gmallreal.app.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.example.gmallreal.common.GmallConfig;
import org.example.gmallreal.common.GmallConstant;

@FunctionHint(output = @DataTypeHint("Row<ct BIGINT,source STRING>"))
public class KeywordProduct2RUDTF extends TableFunction<Row> {
    public void eval(Long clickct, Long cartct, Long orderct){
        if(clickct > 0L){
            Row rowClick = new Row(2);
            rowClick.setField(0,clickct);
            rowClick.setField(1, GmallConstant.KEYWORD_CLICK);
            collect(rowClick);
        }
        if(cartct > 0L){
            Row rowClick = new Row(2);
            rowClick.setField(0,cartct);
            rowClick.setField(1, GmallConstant.KEYWORD_CART);
            collect(rowClick);
        }
        if(orderct > 0L){
            Row rowClick = new Row(2);
            rowClick.setField(0,orderct);
            rowClick.setField(1, GmallConstant.KEYWORD_ORDER);
            collect(rowClick);
        }
    }
}
