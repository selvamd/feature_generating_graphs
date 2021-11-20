package fgg.tests;

import fgg.access.*;
import fgg.data.*;
import java.util.*;
 
public class NodeDataDBTest extends Test 
{
    public NodeDataDBTest(String name) { super(name); }
    
    public void runTest() 
    {
        CBOType    cust = CBOType.valueOf("CUSTOMER");
        FieldMeta  meta = FieldMeta.lookup(cust, "cust_key");
        
        NodeDataDB  node = new NodeDataDB(cust,0,1);
        for (int i=0;i < 510;i++) 
        {
            node.upsert(i+1,meta.index(), 20150101, ""+(i+200));
        }
        System.out.println(node.readRow(150,new HashMap<Integer,Field>()));
        //System.out.println(node.readCol(meta.index(),new HashMap<Integer,Field>()));
    }
}