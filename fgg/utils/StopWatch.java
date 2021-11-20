package fgg.utils;
import java.util.*;
import java.util.concurrent.*;

public class StopWatch 
{
    private static final boolean DEBUG_ON = true;
    private static Map<String,StopWatch> all = new ConcurrentHashMap<String,StopWatch>();
    private final String key;
    private int count = 0;
    private long startms = 0;
    private long totalms = 0;
    
    private StopWatch(String k) {
        this.key = k;
    }

    public String toString() {
        String res = key;
        res += ",totalms=" + totalms;
        res += ",count=" + count;
        if (count > 0)
            res += ",avg=" + (totalms/count);
        return res;
    }
    
    public static void start(String key) {
        StopWatch t = all.get(key);
        if (t == null) all.put(key, t = new StopWatch(key));
        if (DEBUG_ON) t.startms = System.currentTimeMillis();
    }

    public static void stop(String key) {
        StopWatch t = all.get(key);
        if (t == null || t.startms == 0) return;
        t.totalms += System.currentTimeMillis() - t.startms;
        t.count++;
    }
    
    public static void print() {
        if (DEBUG_ON) all.forEach((k,v)->System.out.println(v));
    }
    
}
