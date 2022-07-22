package org.why;

import static org.junit.Assert.assertTrue;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    public static void main(String[] args) throws IOException {
            Expression exp = AviatorEvaluator.getInstance().compileScript("/Users/neilwu/work/test/rule_engine/src/main/resources/rule1.av", true);
        HashMap<String, Object> objectObjectHashMap = new HashMap<>();
        HashMap<String, Object> result = new HashMap<>();

        objectObjectHashMap.put("gen", "男");
        Map<String, Object> resultAndData = new HashMap<>();
        resultAndData.put("data", objectObjectHashMap);
        resultAndData.put("result", result);
        resultAndData = (Map<String, Object>)exp.execute(resultAndData);
        System.out.println("execute = " + resultAndData);
        int a = 1;
        String s = String.valueOf(a);
    }
}
