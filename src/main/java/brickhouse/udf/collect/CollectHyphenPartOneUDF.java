package brickhouse.udf.collect;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Arrays;

/**
 * Retain strings in the first array only if _.split("-")[0]
 * is in the second array.
 */

public class CollectHyphenPartOneUDF extends UDF {

    // in user_session_daily.dw_user_session_daily, the filterSet
    // stays constant once initialized; static filterSet can be used by
    // multiple rows.
    private static Set<String> filterSet = null;

    public List<String> evaluate(List<String> array1, List<String> array2) {
        if(array1 == null) return null;
        if(filterSet == null) {
            filterSet = new HashSet<String>(array2);
        }
        List<String> res = new ArrayList<String>();
        for(String s : array1) {
            if(s == null) continue;
            if(filterSet.contains(s.split("-")[0])) {
                res.add(s);
            }
        }
        return res;
    }

    // mvn exec:java -Dexec.mainClass="brickhouse.udf.collect.CollectHyphenPartOneUDF"
    public static void main(String[] args) {
        CollectHyphenPartOneUDF myudf = new CollectHyphenPartOneUDF();
        // should return ["a-1", "b-2"]
        System.out.println(myudf.evaluate(Arrays.asList("a-1", "b-2", "c-3"), Arrays.asList("a", "b")));
    }
}