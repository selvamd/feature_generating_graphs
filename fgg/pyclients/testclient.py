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

    print("--- Creating new customers and accounts ---")
    id = 100
    for i in range(100):
        store.setObject("Customer", id+i, 1, str(id+i))

    for i in range(10_000):
        store.setObject("Account", id+i, 1, str(id+i))
        store.setObject("Account", id+i, 1, str(id+i))

    #create attr
    print("--- Creating new features age and balance ---")
    store.addAttr(cust.id(),"age", DataType.INT, FieldType.CORE, 4)
    store.addAttr(acct.id(),"balance", DataType.INT, FieldType.CORE, 4)

    #write to the customer object
    print("--- Update age for customer ---")
    cur = store.query("Customer","cust_key,age")
    while cur.next():
        custkey = cur.getObjectPK()
        cur.set("age", asof, 20 + (custkey%50))
        cur.publish()

    #write to the account object
    print("--- Update balance for accounts ---")
    cur = store.query("Account","acct_key,balance")
    while cur.next():
        acctkey = cur.getObjectPK()
        cur.set("balance", asof, (acctkey%35)*1000)
        cur.publish()

    print("--- linking cust objects to account objects ---")
    for i in range(10_000):
        store.setLink(acctcustrel.id(),[int(id+i/10),id+i], asof, 99991231)

    #navigate relationships
    print("---- Now lets query by objects and relationships !! ------")
    #cur = store.query("Customer","cust_key,age","($age > 10)", "+age,+strkey", asof)
    cur = store.query("Customer","cust_key,age","($age > 10)", "", asof)
    while cur.next():
        print("Cust\t",cur.getObjectPK(), cur.get("cust_key",asof), cur.get("age",asof))
        act = cur.linkByName("acctcust", asof)
        act.selectAttrs("acct_key,balance")
        while act.next():
            #act.getLinkAttr("is_valid",asof)
            print("\tAcct\t",act.get("acct_key",asof),act.get("balance",asof))
