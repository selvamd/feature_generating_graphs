package fgg.tests;

import fgg.access.*;
import fgg.data.*;
import java.util.*;
 
public abstract class Test implements Runnable 
{
    private long start = 0;
    private long end = 0;
    private int status = 0;
    
    private static Map<String,Test> tests = new HashMap<String,Test>();

    public Test(String name) 
    {
        if (tests.containsKey(name))
            throw new RuntimeException("Duplicate test : " + name);
        tests.put(name,this);
    }
    
    //public Test() { this(this.getClass().getName()); }        
    

    public void run() {
        try {
            setupTest();
            start = System.currentTimeMillis();
            runTest();
            status = 2;
        }  catch (Exception e) {
            status = 1;
            e.printStackTrace();
        }
        end = System.currentTimeMillis();
    }
    
    public void setupTest() {
		try {
            EnumGroup.load();
            CBOBuilder.buildAll();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    public abstract void runTest();
    
    public static void main(String[] args) throws Exception {
        new NodeDataDBTest("NodeDataDB");
        new EdgeDBTest("EdgeDB");
        new NodeKeyDBTest("NodeKeyDB");
        tests.forEach((k,v)->{
            System.out.println(v.getClass().getName());
            v.run();
        });
    }
}