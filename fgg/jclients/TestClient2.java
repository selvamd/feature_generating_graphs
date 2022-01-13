package fgg.jclients;

import fgg.data.*;
import fgg.grpc.*;
import fgg.utils.*;

public class TestClient2
{

    public static void createClients(ObjectStore store) throws Exception
    {
        //create a new attribute called age in customer object
        GraphItem.Node node = GraphItem.findNode("Customer");
        store.addAttr(node.ordinal(),"age", DataType.INT, FieldType.CORE, 4);

        //Query back the all the created objects
        ObjectCursor cursor = store.query("Customer", "cust_key,age", "", 0);
        if (cursor == null) return;

        //use the cursor to iterate thru the objects
		System.out.println("Iterate all customers and update their age " + cursor.size());
		int count = 0;
        while (cursor.next())
        {
            //set value and publish
            //cursor.set("age", 20150101, ""+ (35+(++count%20)));
            //cursor.publish();
            if (++count % 10000 == 0)
                System.out.println("\t"+cursor.get("cust_key", 20170101)+"->age() = " + cursor.get("age", 20170101));
        }
		System.out.println("Clients created");
    }

	public static void main( String[] args ) throws Exception
	{
        ObjectStore store = ObjectStore.make();
        createClients(store);
	}
}
