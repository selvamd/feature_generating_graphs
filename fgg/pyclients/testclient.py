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

    node = GraphItem.findNode("Customer")
    store.addAttr(node.id(),"age", DataType.INT, FieldType.CORE, 4)
    store.setObject("Customer", 100, "200100")

    cur = store.query("Customer","cust_key,age","")
    while cur.next():
        cur.set("age", 20150101, "100")
        cur.publish()
        break

    '''
    cur = store.query("Customer","cust_key,age","")
    while cur.next():
        act = cur.link("Account", 20170101)
        act.selectAttrs("acct_key,balance")
        while act.next():
            bal = act.get("balance",20190101)
            key = act.get("acct_key",20190101)
            print("\t\t",key,bal)
        key = cur.get("cust_key",20190101)
        age = cur.get("age",20190101)
        print(key,age)
    '''
