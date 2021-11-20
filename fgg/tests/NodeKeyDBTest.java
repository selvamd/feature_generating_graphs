package fgg.tests;

import fgg.access.*;
import fgg.data.*;
import java.util.*;
 
public class NodeKeyDBTest extends Test 
{
    public NodeKeyDBTest(String name) { super(name); }
    
    public void runTest() 
    {
        CBOType    cust = CBOType.valueOf("CUSTOMER");
        NodeKeyDB  node = new NodeKeyDB(cust);
        for (int i=0;i < 510;i++) 
        {
            node.upsert(-1,1,"cust"+i,null);
        }
        System.out.println(node.read("cust150"));
        System.out.println(node.read(250,1));
    }
}