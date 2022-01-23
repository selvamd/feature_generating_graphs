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
    acctcustrel = GraphItem.findEdge("acctcust")

    print("--- Creating customer and accounts ---")
    id = 100
    for i in range(10000):
        store.setObject("Customer", id+i, str(id+i))

    for i in range(100_000):
        store.setObject("Account", id+i, str(id+i))
        store.setObject("Account", id+i, str(id+i))

    #create attr
    print("--- Creating new features age and balance ---")
    store.addAttr(cust.id(),"age", DataType.INT, FieldType.CORE, 4)
    store.addAttr(acct.id(),"balance", DataType.INT, FieldType.CORE, 4)

    #update attr
    print("--- Update age for customer ---")
    cur = store.query("Customer","cust_key,age")
    while cur.next():
        custkey = cur.getObjectPK()
        cur.set("age", asof, 20 + (custkey%50))
        cur.publish()

    #update attr
    print("--- Update balance for accounts ---")
    cur = store.query("Account","acct_key,balance")
    while cur.next():
        acctkey = cur.getObjectPK()
        cur.set("balance", asof, (acctkey%35)*1000)
        cur.publish()

    print("--- linking objects cust to account ---")
    for i in range(100000):
        store.setLink(acctcustrel.id(),[int(id+i/10),id+i], asof,99991231)

    #navigate relationships
    print("---- Now lets query by objects and relationships ------")
    cur = store.query("Customer","cust_key,age","($age > 53)", asof)
    while cur.next():
        print("Cust\t",cur.get("cust_key",asof),cur.get("age",asof))
        act = cur.linkByName("acctcust", asof)
        act.selectAttrs("acct_key,balance")
        while act.next():
            print("\tAcct\t",act.get("acct_key",asof),act.get("balance",asof))
        if cur.getObjectPK() > 200:
            break
