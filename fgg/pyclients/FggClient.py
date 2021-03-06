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
        it = query(self.stub,msg)
        return it.values[2].value

    def getNodes(self):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_NODES
        it = query(self.stub,msg)
        return it.outkey

    def getEdges(self):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_EDGES
        it = query(self.stub,msg)
        return it.outkey

    def getAttrs(self, isnode):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_ATTRS
        FggClient.AddParam(msg, 'isnode', str(isnode).lower())
        it = query(self.stub,msg)
        return it.outkey

    def getNodeInfo(self, nodeid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_NODE_INFO
        FggClient.AddParam(msg, 'nodekey', nodeid)
        it = query(self.stub,msg)
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
        it = query(self.stub,msg)
        edge = Edge(str(edgeid), FggClient.GetParam(it,'name'))
        for x in range(len(it.values)-3):
            edge.addNodeKey(FggClient.GetParam(it, 'nodekey' + str(x)))
        return edge

    def getEdgeAttrInfo(self, attrid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_ATTR_INFO
        FggClient.AddParam(msg, 'attrkey', attrid)
        it = query(self.stub,msg)
        attr = EdgeAttr(attrid, FggClient.GetParam(it, 'name'),
                        GraphItem.findByPK(FggClient.GetParam(it, 'key')),
                        DataType[FggClient.GetParam(it,'dtype')])
        return attr

    def getNodeAttrInfo(self, attrid):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_ATTR_INFO
        FggClient.AddParam(msg, 'attrkey', attrid)
        it = query(self.stub,msg)
        attr = NodeAttr(attrid, FggClient.GetParam(it, 'name'),
                        GraphItem.findByPK(FggClient.GetParam(it, 'key')),
                        DataType[FggClient.GetParam(it, 'dtype')],
                        FggClient.GetParam(it, 'splitbyleg') == 'true')
        return attr

    def getObjPK(self, nodeid, pk):
        obj = self.getObjKeys(nodeid, pk, "", 0)
        return next(iter(obj.keys()))

    def getObjKeys(self, nodeid, matchkey, expr, sort, asofdt):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_OBJ_KEYS
        FggClient.AddParam(msg, 'nodekey', nodeid)
        FggClient.AddParam(msg, 'match', matchkey)
        FggClient.AddParam(msg, 'expr', expr)
        FggClient.AddParam(msg, 'sort', sort)
        FggClient.AddParam(msg, 'asofdt', str(asofdt))
        it = query(self.stub,msg)
        result = {}
        for v in it.outkey:
            result[v] = 1
        for v in it.values:
            if v.name not in "nodekey match expr asofdt sort".split():
                result[v.name] = v.value
        return result

    def setObject(self, nodeid, key):
        return self.setObjectAltKey(nodeid, 0, 1, key)

    def setObjectAltKey(self, nodeid, objid, altkeyseq, key):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.SET_OBJ_KEY
        FggClient.AddParam(msg, 'nodekey', nodeid)
        FggClient.AddParam(msg, 'objkey', objid)
        FggClient.AddParam(msg, 'altkeyseq', altkeyseq)
        FggClient.AddParam(msg, 'str_key', key)
        it = query(self.stub,msg)
        return it.outkey[0]

    def flush(self):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.NOTIFY_FLUSH
        query(self.stub,msg)

    def getLink2Obj(self, edgekey, objs):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_LINK_KEYS
        FggClient.AddParam(msg, 'edgekey', str(edgekey))
        FggClient.AddParam(msg, 'includeobj', "true")
        for i in range(len(attrs)):
            FggClient.AddParam(msg, 'objkey'+ str(i), str(objs[i]))
        it = query(self.stub,msg)
        res = {}
        grps = GraphItem.findByPK(edgekey).maxnodes()+1
        for grp in range(len(it.outkey)/grps):
            obj = []
            for idx in range(grps-1):
                obj.append(it.outkey[grp*grps+idx+1])
            res[it.outkey[grp*grps]] = obj
        return res


    def addAttr(self, nodeedgeid, name, dtype, fldtype, size):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.ADD_ATTR
        FggClient.AddParam(msg, 'nodeedgeid', str(nodeedgeid))
        FggClient.AddParam(msg, 'attrname', name)
        FggClient.AddParam(msg, 'datatype', dtype.name)
        FggClient.AddParam(msg, 'fieldtype', fldtype.name)
        FggClient.AddParam(msg, 'attrsize', str(size))
        it = query(self.stub,msg)
        if len(it.outkey) == 0:
            return False
        self.getNodeAttrInfo(it.outkey[0])
        return True

    def getLink(self, edgekey, linkkeys, attrs):
        res = []
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_OBJECT
        FggClient.AddParam(msg, 'typekey', str(edgekey))
        for idx in range(len(linkkeys)):
            FggClient.AddParam(msg, 'instkey' + str(idx), linkkeys[idx])
        for i in range(len(attrs)):
            FggClient.AddParam(msg, 'attrkey'+ str(i), str(attrs[i].typeid))
        it = request(self.stub,msg)
        v = FeatureData(None)
        for r in it:
            if v.add(r):
                res.append(v)
                v = FeatureData(None)
        return res

    def getObject2(self, edgekey, linkkeys, nodekey, nodecnt, attrs):
        res = []
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_OBJECT
        FggClient.AddParam(msg, 'typekey', str(nodekey))
        for idx in range(len(linkkeys)):
            FggClient.AddParam(msg, 'instkey' + str(idx), linkkeys[idx])
        FggClient.AddParam(msg, 'edgekey', str(edgekey))
        FggClient.AddParam(msg, 'nodecnt', str(nodecnt))
        for i in range(len(attrs)):
            FggClient.AddParam(msg, 'attrkey'+ str(i), str(attrs[i].typeid))
        it = request(self.stub,msg)
        v = FeatureData(None)
        for r in it:
            if v.add(r):
                res.append(v)
                v = FeatureData(None)
        return res

    def getObject(self, nodekey, objkeys, attrs):
        res = []
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_OBJECT
        FggClient.AddParam(msg, 'typekey', nodekey)
        for idx in range(len(objkeys)):
            FggClient.AddParam(msg, 'instkey' + str(idx), objkeys[idx])
        for i in range(len(attrs)):
            FggClient.AddParam(msg, 'attrkey'+ str(i), str(attrs[i].typeid))
        it = request(self.stub,msg)
        v = FeatureData(None)
        for r in it:
            if v.add(r):
                res.append(v)
                v = FeatureData(None)
        return res

    def getLinkKeys(self, edgeid, objkeys, asofdt):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_LINK_KEYS
        FggClient.AddParam(msg, 'edgekey', edgeid)
        FggClient.AddParam(msg, 'asofdt', str(asofdt))
        FggClient.AddParam(msg, 'includeobj', 'false')
        for i in range(len(objkeys)):
            FggClient.AddParam(msg, 'objkey'+str(i), objkeys[i])
        it = query(self.stub,msg)
        return it.outkey

    def setLink(self, edgeid, objkeys, fromdt, todt):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.SET_LINK_KEY
        FggClient.AddParam(msg, 'edgekey', edgeid)
        FggClient.AddParam(msg, 'fromdt', fromdt)
        FggClient.AddParam(msg, 'todt', todt)
        for i in range(len(objkeys)):
            FggClient.AddParam(msg, 'objkey'+str(i), objkeys[i])
        it = query(self.stub,msg)
        return it.outkey[0]

    def publish(self, features):
        input = []

        for feature in features:
            for xmit in feature.xmits():
                input.append(xmit)

        it = self.stub.persistData(iter(input))
        for v in it.values:
            if v.name == 'UPDATED':
                return v.value

    def getDates(self):
        msg = FggDataService_pb2.FggMsg()
        msg.request = MsgType.GET_DATES
        it = query(self.stub,msg)
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
        nodes = self.getNodes()
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

def request(stub, msg):
    #print(str(msg))
    return stub.requestData(msg)

def query(stub, msg):
    #print(str(msg))
    return stub.queryData(msg)

if __name__ == '__main__':
    client = FggClient('localhost',33789)
    client.connect()
    cust = GraphItem.findByName('Customer')
    print("Lookup By Name -> " + str(cust))
    print("Lookup By ID -> " + str(GraphItem.findByPK(cust.typeid)))
    print("Lookup Attrs -> ")
    attrs = GraphItem.findNodeAttrs(cust.typeid)
    for attr in attrs:
        print(attr)
    print("Lookup Object -> ")
    key = client.getObjPK(cust.typeid, "200010");
    objs = client.getObject(cust.typeid, key, [attrs[0].typeid]);
    for obj in objs:
        print(str(obj))
