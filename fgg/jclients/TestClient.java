package fgg.jclients;

import fgg.data.*;
import fgg.grpc.*;

public class TestClient
{
    public static int uniquekey = 100;

    public static void custBalance(ObjectStore store) throws Exception
    {
        ObjectCursor cust_cursor = store.query("Customer", "cust_key", "");
        if (cust_cursor == null) return;

        while (cust_cursor.next())
        {
            ObjectCursor acct_cursor = cust_cursor.link("Account", 20170101);
            acct_cursor.selectAttrs("acct_key,balance");
            long bal = 0;
            while (acct_cursor.next())
            {
                int i = Integer.parseInt(acct_cursor.get("balance",20190101));
                bal += i;
            }

            int objpk = cust_cursor.getObjectPk();
            String custkey = cust_cursor.get("cust_key",20190101);
            System.out.println(objpk +",custkey=" + custkey + ",balance=" + bal);
        }
		System.out.println("Computed client balance from account balances");
    }

    //This function iterates thru all the account objects and adds links to client objects
    public static void setupLinks(ObjectStore store) throws Exception
    {
        ObjectCursor cursor = store.query("Account", "acct_key,balance", "");
        if (cursor == null) return;

        GraphItem.Node[] nodes = new GraphItem.Node[] { GraphItem.findNode("Account"), GraphItem.findNode("Customer") };
        GraphItem.Edge edge = GraphItem.findDefaultEdge(nodes);

        while (cursor.next())
        {
            int i = Integer.parseInt(cursor.get("acct_key",20190101));
            String custkey = ""+(200000 + (i%100));
            //System.out.println("custkey=" + store.getObjectPk("Customer", custkey) + ",custkey=" + custkey + ",acctkey=" + cursor.get("acct_key",20170101));
            int ckey = store.getObjectPk("Customer", custkey);
            //cursor.addLink("Customer", ckey, 20150101, 99991231);
            store.setLink(edge, new int[] { ckey, cursor.getObjectPk() }, 20150101, 99991231);
        }
		System.out.println("Clients and Accounts linked");
    }

    public static void createAccounts(ObjectStore store) throws Exception
    {
        GraphItem.Node node = GraphItem.findNode("Account");
        store.addAttr(node.ordinal(),"balance", DataType.INT, FieldType.CORE, 4);

        for (int i=0;i<1000;i++)
            store.setObject("Account", uniquekey++, ""+(100000 + i));

        //Create 1000 account objects
        ObjectCursor cursor = store.query("Account", "acct_key,balance", "");
        if (cursor == null) return;

        //use the cursor to get and set values
        int idx = 0;
        while (cursor.next())
        {
            idx = (++idx % 25)+1;
            cursor.set("balance", 20150101, "" + (idx*10000));
            cursor.publish();
            //System.out.println(cursor.get("acct_key", 20170101)+":"+cursor.get("balance", 20170101));
        }

		System.out.println("Accounts created");
    }

    public static void createClients(ObjectStore store) throws Exception
    {
        //create a new attribute called age in customer object
        GraphItem.Node node = GraphItem.findNode("Customer");
        store.addAttr(node.ordinal(),"age", DataType.INT, FieldType.CORE, 4);

        //GraphItem.NodeAttr attr = GraphItem.findAttr(GraphItem.findNode("Customer"), "age");
        //if (attr != null) System.out.println("Attr created");

        //Create 100 customer objects
        for (int i=0;i<100;i++)
            store.setObject("Customer", uniquekey++, ""+(200000 + i));

        //Query back the all the created objects
        ObjectCursor cursor = store.query("Customer", "cust_key,age", "");
        if (cursor == null) return;

        //use the cursor to iterate thru the objects
        while (cursor.next())
        {
            //set value and publish
            cursor.set("age", 20150101, "50");
            cursor.publish();
            //System.out.println(cursor.get("cust_key", 20170101)+":"+cursor.get("age", 20170101));
        }
		System.out.println("Clients created");
    }

    public static void describe(ObjectStore store) {
        String[] objects = store.getObjectNames();
        for (String obj:objects) {
            System.out.println(obj);
            String[] attrs = store.getAttrNames(obj);
            for (String attr:attrs)
                System.out.println("\t"+attr + "\t" + store.getDataType(obj,attr));
        }
        System.out.println("");
    }

	public static void main( String[] args ) throws Exception
	{
        ObjectStore store = ObjectStore.make();
        store.printSchema();

        createClients(store);
        createAccounts(store);
        setupLinks(store);
        custBalance(store);
        Thread.sleep(1000);
	}
}
