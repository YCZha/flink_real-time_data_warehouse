package org.example.gmallreal.app.func;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.example.gmallreal.utils.KeywordUtil;

import java.util.List;


//因为flink本身不支持使用自定义函数，所以需要自定义一个函数去继承tableFunction，来实现
@FunctionHint(output = @DataTypeHint("Row<word STRING>"))
public class KeywordUDTF extends TableFunction<Row>{
    public void eval(String value){
        //使用工具类对字符串进行分词
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            Row row = new Row(1);
            row.setField(0,keyword);
            collect(row);
        }
    }
}
