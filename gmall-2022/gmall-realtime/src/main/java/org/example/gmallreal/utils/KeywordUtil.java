package org.example.gmallreal.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

//IK分词器分词工具类
public class KeywordUtil {
    //分词，将字符串进行分词，将分词之后的结果放到一个集合中返回
    public static List<String> analyze(String text){
        List<String> wordLit = new ArrayList<>();
        //将刺符传转化为字符输入流
        StringReader reader = new StringReader(text);
        //创建分词器对象
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);


        //lexeme是一个单词对象
        Lexeme lexeme = null;
        //通过循环，获取分词后数据
        while(true){
            try {
                //获取一个单词
                if((lexeme = ikSegmenter.next()) != null){
                    String word = lexeme.getLexemeText();
                    wordLit.add(word);
                }
                else{
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return wordLit;
    }

    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信 4G 手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));

    }
}
