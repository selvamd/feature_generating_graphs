import grpc
import FggDataService_pb2
import FggDataService_pb2_grpc
from enum_types import *
from GraphItem import *
from FeatureData import *

class FggClient:

    def __init__(self, host, port):
        self.channel = grpc.insecure_channel(host+':'+str(port))
        self.stub = FggDataService_pb2_grpc.FggDataServiceStub(self.channel)

    def login(self, user, passwd):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.LOGIN
        FggClient.AddParam(msg, 'user', user)
        FggClient.AddParam(msg, 'pass', passwd)
        it = self.stub.queryData(msg)
        return it.values[2].value

    def getNodes(self, baseNode):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_NODES
        it = self.stub.queryData(msg)
        return it.outkey

    def getEdges(self):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_EDGES
        it = self.stub.queryData(msg)
        return it.outkey

    def getAttrs(self, isnode):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_ATTRS
        FggClient.AddParam(msg, 'isnode', str(isnode).lower())
        it = self.stub.queryData(msg)
        return it.outkey

    def getNodeInfo(self, nodeid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_NODE_INFO
        FggClient.AddParam(msg, 'nodekey', nodeid)
        it = self.stub.queryData(msg)
        parent = FggClient.GetParam(it,'parentnodekey')
        leg = FggClient.GetParam(it,'legnodekey')
        root = FggClient.GetParam(it,'rootnodekey')
        if root == nodeid:
            node = RootNode(str(nodeid), FggClient.GetParam(it,'name'))
            node.setLegKey(leg)
            return node
        else:
            node = Node(str(nodeid), FggClient.GetParam(it,'name'))
            node.setParentKey(parent)
            return node

    def getEdgeInfo(self, edgeid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_EDGE_INFO
        FggClient.AddParam(msg, 'edgekey', edgeid)
        it = self.stub.queryData(msg)
        edge = Edge(str(edgeid), FggClient.GetParam(it,'name'))
        for x in range(len(it.values)-2):
            edge.addNodeKey(FggClient.GetParam(it, 'node' + str(x)))
        return edge

    def getEdgeAttrInfo(self, attrid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_ATTR_INFO
        FggClient.AddParam(msg, 'attrkey', attrid)
        it = self.stub.queryData(msg)
        attr = EdgeAttr(attrid, FggClient.GetParam(it, 'name'),
                        GraphItem.findByPK(FggClient.GetParam(it, 'key')),
                        DataType[FggClient.GetParam(it,'dtype')])
        return attr

    def getNodeAttrInfo(self, attrid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_ATTR_INFO
        FggClient.AddParam(msg, 'attrkey', attrid)
        it = self.stub.queryData(msg)
        attr = NodeAttr(attrid, FggClient.GetParam(it, 'name'),
                        GraphItem.findByPK(FggClient.GetParam(it, 'key')),
                        DataType[FggClient.GetParam(it, 'dtype')],
                        FggClient.GetParam(it, 'splitbyleg') == 'true')
        return attr

    def getObjPK(self, nodeid, pk):
        obj = self.getObjKeys(nodeid, pk)
        return next(iter(obj.keys()))

    def getObjKeys(self, nodeid, matchkey):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_OBJ_KEYS
        FggClient.AddParam(msg, 'nodekey', nodeid)
        FggClient.AddParam(msg, 'matchkey', matchkey)
        it = self.stub.queryData(msg)
        result = {}
        for v in it.outkey:
            result[v] = 1
        for v in it.values:
            if v.name != 'nodekey' and v.name != 'matchkey':
                result[v.name] = v.value
        return result

    def getObject(self, nodekey, objkey):
        res = []
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_OBJECT
        FggClient.AddParam(msg, 'typekey', nodekey)
        FggClient.AddParam(msg, 'instkey', objkey)
        attr = GraphItem.findNodeAttrs(nodekey)
        for i in range(len(attr)):
            FggClient.AddParam(msg, 'attrkey'+ str(i), str(attr[i].typeid))
        it = self.stub.requestData(msg)
        v = FeatureData(None)
        for r in it:
            if v.add(r):
                res.append(v)
                v = FeatureData(None)
        return res

    def setObject(self, nodeid, key):
        return self.setObjectAltKey(nodeid, 0, 1, key)

    def setObjectAltKey(self, nodeid, objid, altkeyseq, key):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.SET_OBJ_KEY
        FggClient.AddParam(msg, 'nodekey', nodeid)
        FggClient.AddParam(msg, 'objkey', objid)
        FggClient.AddParam(msg, 'altkeyseq', altkeyseq)
        FggClient.AddParam(msg, 'str_key', key)
        it = self.stub.queryData(msg)
        return it.outkey[0]

    def getLink(self, edgeid, objkeys):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_LINK_KEYS
        FggClient.AddParam(msg, 'edgekey', edgeid)
        for i in range(len(objkeys)):
            FggClient.AddParam(msg, 'nodekey'+str(i), objkeys[i])
        it = self.stub.queryData(msg)
        return it.outkey

    def setLink(self, edgeid, objkeys, fromdt, todt):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.SET_LINK_KEY
        FggClient.AddParam(msg, 'edgekey', edgeid)
        FggClient.AddParam(msg, 'fromdt', fromdt)
        FggClient.AddParam(msg, 'todt', todt)
        for i in range(len(objkeys)):
            FggClient.AddParam(msg, 'nodekey'+str(i), objkeys[i])
        it = self.stub.queryData(msg)
        return it.outkey[0]

    def pulishFeatures(self, features):
        input = []
        for feature in features:
            for xmit in feature.xmits():
                input.append(xmit)
        it = stub.persistData(input)
        for v in it.values:
            if v.name == 'UPDATED':
                return v.value

    def getDates(self):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_DATES
        it = self.stub.queryData(msg)
        return it.outkey

    @staticmethod
    def GetParam(msg, name):
        for x in range(len(msg.values)):
            if msg.values[x].name == name:
                return msg.values[x].value

    @staticmethod
    def AddParam(msg, name, value):
        p = msg.values.add()
        p.name = str(name)
        if value is None:
            value = ''
        p.value = str(value)

    def connect(self):
        self.login("qwer","dsd")
        nodes = self.getNodes(False)
        for node in nodes:
            self.getNodeInfo(node)
        edges = self.getEdges()
        for edge in edges:
            self.getEdgeInfo(edge)
        attrs = self.getAttrs(True)
        for attr in attrs:
            self.getNodeAttrInfo(attr)
        attrs = self.getAttrs(False)
        for attr in attrs:
            self.getEdgeAttrInfo(attr)

if __name__ == '__main__':
    client = FggClient('localhost',33789)
    client.connect()
    cust = GraphItem.findByName('Customer')
    print("Lookup By Name -> " + str(cust))
    print("Lookup By ID -> " + str(GraphItem.findByPK(cust.typeid)))
    print("Lookup Attrs -> ")
    for attr in GraphItem.findNodeAttrs(cust.typeid):
        print(attr)
    key = client.getObjPK(cust.typeid, "200010");
    objs = client.getObject(cust.typeid, key);
    for obj in objs:
        print(str(obj))
