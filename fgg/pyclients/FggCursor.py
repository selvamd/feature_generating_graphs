import grpc
import FggDataService_pb2
import FggDataService_pb2_grpc
from enum_types import *
from GraphItem import *
from FeatureData import *
from FggClient import *

class FggCursor:

    def __init__(self, cli, node, keys, edge = None, cnt = 0):
        self.client = cli
        self.edge = edge
        self.node = node
        self.nodecnt = cnt
        self.result = keys
        self.idx = -1
        self.features = []
        self.attrs = []
        self.edgeattrs = []

    def link(nodename, asof):
        node = GraphItem.findNode(objectName)
        if node is None: return None
        edge = GraphItem.findDefaultEdge([info, node])
        if edge is None: return None
        objid = [self.result[self.idx],0] if edge.index(node) == 0 else [0,self.result[self.idx]]
        fks = self.client.getLinkKeys(edge.typeid, objid)
        cnt = 1 if info.typeid == node.typeid else 0
        return FggCursor(self.client, info, fks, edge, cnt)

    def linkByName(linkname, asof):
        edge = GraphItem.findEdge(linkname)
        if edge is None: return None
        info = edge.node((edge.index(node)+1)%2)
        objid = [0,0]
        objid[edge.index(node)] = self.result[self.idx]
        fks = self.client.getLinkKeys(edge.typeid, objid)
        cnt = 1 if info.typeid == node.typeid else 0
        return FggCursor(self.client, info, fks, edge, cnt)

    def keys(self):
        return self.result

    def next(self):
        self.idx += 1
        self.__fetch__()
        return self.idx < len(self.result)

    def selectAttrs(self, attrs):
        for attr in attrs.split(","):
            atg = GraphItem.findNodeAttr(attr)
            if atg is None: return False
            self.attrs.append(atg)
        self.__fetch__()
        return True

    def selectLinkAttrs(self, attrs):
        for attr in attrs.split(","):
            atg = GraphItem.findEdgeAttr(attr)
            if atg is None: return False
            self.edgeattrs.append(atg)
        self.__fetch__()
        return True

    def getObjectPk(self):
        return self.result[self.idx]

    def getLinkPk(self):
        return self.result[self.idx]

    def __fetch__(self):
        nidx = self.node.typeid
        eidx = self.edge.typeid
        id = self.result[self.idx]
        if edge is not None and len(self.attrs) > 0:
            self.features = self.client.getObject(eidx, id, nidx, self.attrs)
        elif edge is not None:
            self.features = self.client.getLink(eidx, id, self.edgeattrs)
        else:
            self.features = self.client.getObject(nidx, id, self.attrs)

    def __feat__(self, name):
        attrkey = 0
        for a in self.attrs:
            if a.typename == attr:
                attrkey = a.typeid
        for a in self.edgeattrs:
            if a.typename == attr:
                attrkey = a.typeid
        for feat in self.features:
            if feat.attrkey == attrkey:
                return feat
        return None

    def get(attr, asof):
        feat = self.__feat__(attr)
        return None if feat is None else feat.getValue(asof)

    def set(attr, asof, val):
        feat = self.__feat__(attr)
        if feat is not None:
            feat.setValue(asof,val)

    def getScdDates(self):
        s = set()
        for feat in self.features:
            feat.getScdDates(s)
        return s

    def publish(self):
        self.client.publish(self.features)

if __name__ == '__main__':
    client = FggClient('localhost',33789)
    client.connect()
    store = FggStore(client)
    store.printSchema()
