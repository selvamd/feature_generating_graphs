""" represents type system for the CBOSS API"""
class GraphItem:
    """ Base class for all the types with nodeid and nodename"""
    graphitems = []
    def __init__(self, typeid, name):
        self.typeid = typeid
        self.typename = name
        GraphItem.graphitems.append(self)

    def id(self): return self.typeid
    def name(self): return self.typename

    def __str__(self):
        return str(self.typeid) + ':' + self.typename

    ################ GRAPHITEM OBJECT ACCESSORS ######################
    @classmethod
    def findByPK(cls, typeid):
        """ retrieves a base node """
        for item in cls.graphitems:
            if item.typeid == typeid:
                return item
        return None

    @classmethod
    def findNode(cls, typename):
        """ retrieves a base node """
        for item in cls.graphitems:
            if isinstance(item, Node) and item.typename == typename:
                return item
        return None

    @classmethod
    def findEdge(cls, typename):
        """ retrieves a base node """
        for item in cls.graphitems:
            if isinstance(item, Edge) and item.typename == typename:
                return item
        return None

    @classmethod
    def findByName(cls, typename):
        """ retrieves a base node """
        for item in cls.graphitems:
            if item.typename == typename:
                return item
        return None

    @classmethod
    def findByType(cls, clz):
        """ retrieves by type """
        results = []
        for item in cls.graphitems:
            if isinstance(item, clz):
                results.append(item)
        return results

    @classmethod
    def findNodeAttr(cls, nodeid, name):
        """ retrieves attrs for a node """
        for item in cls.graphitems:
            if isinstance(item, NodeAttr):
                if item.owner.typeid == nodeid:
                    if item.typename == name:
                        return item
        return None

    @classmethod
    def findEdgeAttr(cls, edgeid, name):
        """ retrieves attrs for a node """
        results = []
        for item in cls.graphitems:
            if isinstance(item, EdgeAttr):
                if item.owner.typeid == edgeid:
                    if item.typename == name:
                        return item
        return None

    @classmethod
    def findNodeAttrs(cls, nodeid):
        """ retrieves attrs for a node """
        results = []
        for item in cls.graphitems:
            if isinstance(item, NodeAttr):
                if item.owner.typeid == nodeid:
                    results.append(item)
        return results

    @classmethod
    def findDefaultEdge(cls, nodeids):
        """ retrieves attrs for a node """
        results = []
        for item in cls.graphitems:
            if isinstance(item, Edge):
                if item.typename == str(nodeids[0])+"_"+str(nodeids[1]):
                    return item
                if item.typename == str(nodeids[1])+"_"+str(nodeids[0]):
                    return item
        return None

    @classmethod
    def findEdgeAttrs(cls, edgeid):
        """ retrieves attrs for a node """
        results = []
        for item in cls.graphitems:
            if isinstance(item, EdgeAttr):
                if item.owner.typeid == edgeid:
                    results.append(item)
        return results

################ CHILD CLASS DEFINITIONS ######################
class Node(GraphItem):
    def __init__(self, typeid, name):
        super().__init__(typeid, name)

    def setParentKey(self, node):
        self.parent = node

class RootNode(Node):
    def __init__(self, typeid, name):
        super().__init__(typeid, name)

    def setLegKey(self, node):
        self.leg = node

class Edge(GraphItem):
    def __init__(self, typeid, name):
        super().__init__(typeid, name)
        self.nodes = []

    def maxnodes(self):
        return len(self.nodes)

    def node(self, idx):
        return self.nodes[idx]

    def index(self, node):
        for idx in range(len(self.nodes)):
            if self.nodes[idx] == node.typeid:
                return idx
        return -1

    def addNodeKey(self, node):
        self.nodes.append(node)

    def __str__(self):
        res = str(self.typeid) + ':' + self.typename + ":["
        for n in self.nodes:
            res += str(n) + ","
        res += "]"
        return res

class NodeAttr(GraphItem):
    def __init__(self, typeid, name, ownerNodeobj, dtypeEnum, splitbyleg):
        super().__init__(typeid, name)
        self.owner = ownerNodeobj
        self.dtype = dtypeEnum
        self.splitbyleg = splitbyleg

    def __str__(self):
        return str(self.typeid) + ':' + self.typename + ':' + str(self.dtype)[9:]

class EdgeAttr(GraphItem):
    def __init__(self, typeid, name, ownerEdgeobj, dtypeEnum):
        super().__init__(typeid, name)
        self.owner = ownerEdgeobj
        self.dtype = dtypeEnum

    def __str__(self):
        return str(self.typeid) + ':' + self.typename + ':' + str(self.dtype)[9:]
