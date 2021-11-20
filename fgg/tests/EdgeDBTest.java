package fgg.tests;

import fgg.access.*;
import fgg.data.*;
import java.util.*;
 
public class EdgeDBTest extends Test 
{
    public EdgeDBTest(String name) { super(name); }
    
    public void runTest() 
    {
        CBOType  cust = CBOType.valueOf("CUSTOMER");
        CBOType  acct = CBOType.valueOf("ACCOUNT");
        LinkType type = LinkType.valueOf(cust,acct);
        EdgeDB   edge = new EdgeDB(type);
        int[] keys    = new int[5];
        for (int i=0;i < 510;i++) 
        {
            keys[0] = keys[1] = i+1;
            edge.upsert(keys,20150101,99991231);
        }
        System.out.println(edge.read(new int[] { 150,150,0,0,0 }, true, new HashSet<Integer>()));
        System.out.println(edge.read(new int[] { 505,505,0,0,0 }, true, new HashSet<Integer>()));
    }
}