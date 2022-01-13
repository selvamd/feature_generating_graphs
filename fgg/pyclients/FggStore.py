import grpc
import FggDataService_pb2
import FggDataService_pb2_grpc
from enum_types import *
from GraphItem import *
from FeatureData import *
from FggClient import *
from FggCursor import *

class FggStore:

    def __init__(self, host = 'localhost', port = 33789):
        client = FggClient(host,port)
        client.connect()
        self.client = client

    #Returns all the object names in the store
    def getObjectNames(self):
        nodes = GraphItem.findByType(RootNode)
        res = []
        for node in nodes:
            res.append(node.typename)
        return res

    #Returns all the attr names for the objects in the store
    def getAttrNames(self, objectName):
        info = GraphItem.findNode(objectName)
        if info is None: return -1
        attrs = GraphItem.findNodeAttrs(info.typeid)
        res = []
        for attr in attrs:
            res.append(attr.typename)
        return res

    #Returns dtype of a single object attribute
    def getDataType(self, objectName, attrName):
        info = GraphItem.findNode(objectName)
        if info is None: return -1
        attr = GraphItem.findNodeAttr(info.typeid, attrName)
        if attr is None: return -1
        return attr.dtype

    #Returns data size of a single object attribute
    def getDataSize(self, objectName, attrName):
        info = GraphItem.findNode(objectName)
        if info is None: return -1
        attr = GraphItem.findNodeAttr(info.typeid, attrName)
        if attr is None: return -1
        print ("Not implemented")
        return -1

    def sendFlush(self):
        self.client.flush()

    #Runs a query on single object collection and returns a cursor to iterate all the objects
    def query(self, objectName, selects, filter = "", asofdt = 0):
        info = GraphItem.findNode(objectName)
        if info is None: return None;
        res = []
        for key in self.client.getObjKeys(info.typeid, "", filter, asofdt):
            res.append(key)
        if selects is None:
            for attr in GraphItem.findNodeAttrs(info.typeid):
                selects = '' if selects is None else selects + ","
                selects += attr.typename
        cur = FggCursor(self.client, info, res)
        cur.selectAttrs(selects)
        return cur


    #typekey = node or edge key
    def addAttr(self, typekey, name, dtype, ftype, size):
        self.client.addAttr(typekey, name, dtype, ftype, size)

    def getObjectPK(self, objectName, key):
        info = GraphItem.findNode(objectName)
        if info is None: return -1
        return self.client.getObjPK(info.typeid, key)

    #returns map of linkkeys -> objkeys[]
    def getLink2Obj(self, edgekey, objids):
        return self.client.getLink2Obj(edgekey, objids)

    def getLinks(self, edgekey, objids):
        return self.client.getLinkKeys(edgekey, objid, 0)

    def setObject(self, objectName, objpk, key):
        info = GraphItem.findNode(objectName)
        if info is None: return -1
        return self.client.setObjectAltKey(info.typeid, objpk, 1, key)

    def setObjectAltKey(self, objectName, objid, keyseq, key):
        info = GraphItem.findNode(objectName)
        if info is None: return -1
        return self.client.setObjectAltKey(info.typeid, objid, keyseq, key)

    def setLink(self, edge, objids, fromdt, todt):
        return self.client.setLink(edge, objids, fromdt, todt)

    def printSchema(self):
        print("--- OBJECTS ---")
        for node in GraphItem.findByType(Node):
            print(node.typename + "(" + str(node.typeid) + ")")
            for attr in GraphItem.findNodeAttrs(node.typeid):
                print('\t',attr.typename + "(" + str(attr.typeid) + ")", attr.dtype)
        print("--- RELATIONSHIPS ---");
        for node in GraphItem.findByType(Edge):
            print(node.typename + "(" + str(node.typeid) + ")")
            for attr in GraphItem.findEdgeAttrs(node.typeid):
                print('\t',attr.typename + "(" + str(attr.typeid) + ")", attr.dtype)
