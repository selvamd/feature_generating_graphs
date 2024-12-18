package fgg.jclients;

import fgg.data.*;
import fgg.grpc.*;
import fgg.utils.*;

public class TestClient
{
    public static int uniquekey = 100;

    public static void custBalance(ObjectStore store) throws Exception
    {
        ObjectCursor cust_cursor = store.query("Customer", "cust_key,age", "", 0);
        if (cust_cursor == null) return;

        while (cust_cursor.next())
        {
            ObjectCursor acct_cursor = cust_cursor.link("Account", 20170101);
            acct_cursor.selectAttrs("acct_key,balance");
            long bal = 0;int count = 0;
            while (acct_cursor.next())
            {
                int i = Integer.parseInt(acct_cursor.get("balance",20190101));
                bal += i;
				count++;
	            System.out.println("\t"+"acctkey=" + acct_cursor.get("acct_key",20190101) + ",balance=" + i);
            }

            int objpk = cust_cursor.getObjectPk();
            String custkey = cust_cursor.get("cust_key",20190101);
            String age = cust_cursor.get("age",20190101);
            System.out.println(objpk +",custkey=" + custkey + ",custage=" + age + ",cust_balance=" + bal +",acct_count="+count);
        }
		System.out.println("Computed client balance from account balances");
    }

    //This function iterates thru all the account objects and adds links to client objects
    public static void setupLinks(ObjectStore store) throws Exception
    {
        ObjectCursor cursor = store.query("Account", "acct_key,balance", "", 0);
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
			System.out.println("\tAssign acctkey " + cursor.getObjectPk() + " to cust key " + ckey);
            store.setLink(edge, new int[] { ckey, cursor.getObjectPk() }, 20150101, 99991231);
        }
		System.out.println("Clients and Accounts linked");
    }

    public static void createAccounts(ObjectStore store) throws Exception
    {
        GraphItem.Node node = GraphItem.findNode("Account");
        store.addAttr(node.ordinal(),"balance", DataType.INT, FieldType.CORE, 4);

		System.out.println("Adding 1000 new account objects");
		System.out.println("\tFrom " + 100000);
		System.out.println("\tTo   " + 101000);

        for (int i=0;i<1000;i++)
            store.setObject("Account", uniquekey++, ""+(100000 + i));


        //Create 1000 account objects
        ObjectCursor cursor = store.query("Account", "acct_key,balance", "", 0);
        if (cursor == null) return;

        //use the cursor to get and set values
        int idx = 0;
		System.out.println("Iterate the accounts and update their balances");
        while (cursor.next())
        {
            idx = (++idx % 25)+1;
            cursor.set("balance", 20150101, "" + (idx*10000));
            cursor.publish();
            System.out.println("\t"+cursor.get("acct_key", 20170101)+"->balance() = "+cursor.get("balance", 20170101));
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
		System.out.println("Adding 100 new customer objects");
		System.out.println("\tFrom " + 200000);
        for (int i=0;i<100;i++)
            store.setObject("Customer", uniquekey++, ""+(200000 + i));
		System.out.println("\tTo   " + 200100);

        //Query back the all the created objects
        ObjectCursor cursor = store.query("Customer", "cust_key,age", "", 0);
        if (cursor == null) return;

        //use the cursor to iterate thru the objects
		System.out.println("Iterate all customers and update their age");
		int count = 0;
        while (cursor.next())
        {
            //set value and publish
            cursor.set("age", 20150101, ""+ (35+(++count%20)));
            cursor.publish();
            System.out.println("\t"+cursor.get("cust_key", 20170101)+"->age() = " + cursor.get("age", 20170101));
        }
		System.out.println("Clients created");
    }

	public static void main( String[] args ) throws Exception
	{
        ObjectStore store = ObjectStore.make();
		String cmd = null;
		while (true) {
			cmd = Utils.prompt("Input Command ");
			if (cmd.equals("schema"))
				store.printSchema();
			if (cmd.equals("new_cust"))
				createClients(store);
			if (cmd.equals("new_acct"))
				createAccounts(store);
			if (cmd.equals("acct_cust_rel"))
				setupLinks(store);
			if (cmd.equals("cust_bal"))
				custBalance(store);
			if (cmd.equals("help"))
				for (String s:"schema,new_cust,new_acct,acct_cust_rel,cust_bal".split(","))
				System.out.println(s);
			if (cmd.equals("quit") || cmd.equals("exit"))
				System.exit(-1);
		}
	}
}

