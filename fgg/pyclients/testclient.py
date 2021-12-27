from enum_types import *
from GraphItem import *
from FggClient import *
from FggCursor import *
from FggStore import *

if __name__ == '__main__':

    client = FggClient('localhost',33789)
    client.connect()
    store = FggStore(client)

    store.printSchema()

    cust = GraphItem.findNode("Customer")
    acct = GraphItem.findNode("Account")
    ac_cs_rel = GraphItem.findDefaultEdge([cust.id(),acct.id()])

    #create attr
    store.addAttr(cust.id(),"age", DataType.INT, FieldType.CORE, 4)

    #create obj
    store.setObject("Customer", 100, "200100")

    #update attr
    cur = store.query("Customer","cust_key,age","", 0)
    while cur.next():
        cur.set("age", 20150101, "100")
        cur.publish()
        break

    #setup relationship
    actcur = store.query("Account","acct_key","", 0)
    custkey = store.getObjectPK("Customer","200100")
    while actcur.next():
        store.setLink(ac_cs_rel.id(),[custkey,actcur.getObjectPK()], 20150101,99991231)
        break

    #navigate relationships
    cur = store.query("Customer","cust_key,age","($age > 53)", 20150101)
    while cur.next():
        act = cur.link("Account", 20160101)
        act.selectAttrs("acct_key,balance")
        while act.next():
            bal = act.get("balance",20190101)
            key = act.get("acct_key",20190101)
            print("\t\t",key,bal)
        key = cur.get("cust_key",20190101)
        age = cur.get("age",20190101)
        print(key,age)
