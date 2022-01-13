from enum_types import *
from GraphItem import *
from FggCursor import *
from FggStore import *

if __name__ == '__main__':

    # Create graphstore client
    asof = 20150101
    store = FggStore()
    store.printSchema()

    # Lookup objects and relationships
    cust = GraphItem.findNode("Customer")
    acct = GraphItem.findNode("Account")
    acctcustrel = GraphItem.findDefaultEdge([cust.id(),acct.id()])

    print("--- Creating 1 customer and 2 accounts ---")
    store.setObject("Customer", 100, "200000")
    store.setObject("Account", 101, "500000")
    store.setObject("Account", 102, "500001")

    #create attr
    print("--- Creating new features age and balance ---")
    store.addAttr(cust.id(),"age", DataType.INT, FieldType.CORE, 4)
    store.addAttr(acct.id(),"balance", DataType.INT, FieldType.CORE, 4)

    #update attr
    print("--- Update age for customer ---")
    cur = store.query("Customer","cust_key,age")
    while cur.next():
        cur.set("age", asof, 100)
        cur.publish()

    #update attr
    print("--- Update balance for accounts ---")
    cur = store.query("Account","acct_key,balance")
    while cur.next():
        cur.set("balance", asof, 1000)
        cur.publish()

    print("--- linking objects cust to account ---")
    store.setLink(acctcustrel.id(),[100,101], asof,99991231)
    store.setLink(acctcustrel.id(),[100,102], asof,99991231)

    #navigate relationships
    print("---- Now lets query ------")
    cur = store.query("Customer","cust_key,age","($age > 53)", asof)
    while cur.next():
        print("Cust\t",cur.get("cust_key",asof),cur.get("age",asof))
        act = cur.link("Account", asof)
        act.selectAttrs("acct_key,balance")
        while act.next():
            print("\tAcct\t",act.get("acct_key",asof),act.get("balance",asof))
