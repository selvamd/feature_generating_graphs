# feature_generating_graphs
FGG is a read-write object graph framework for data virtualization, dynamic read-writes, traversals and feature computations

## FGG is a structured graph 
	- Node is the prototype for objects (or conversely objects are instances of a node) 
	- Edges are defined by connection between a set of nodes and therefore are prototypes for relationships.
	- Analogous to objects for node, link refers to an instance of an edge (or a relationship)
	- Edges connecting more than 2 nodes are possible and create what are called as Hypergraphs

Attributes can be added to both nodes and edges and its instances are timeseries objects and not scalar values  

## 
FGG publishes language specific client library that abstracts the low level connectivity details to the remote object store.   
This section describes how to use the client API to interact with the graph 

## Basic interactions with object graph 
	- Create an instance of FggStore (Clientside reference to interact with graphstore)
		store = FggStore()
	- Create new Object instance of a particular node type
		objid = store.setObject(nodename, Key)
	- Setup relationship between 2 objects
		store.setLink(edgename,[obj1.id,obj2.id], fromdt, todt)
	- Define new Attribute for a node or an edge 
		store.addAttr(node_edge_name, attrname, datatype..)
	- Obtain cursor to objects in object store
		cursor = store.query(nodename, attributes, filter, sort)
	- Iterate and read data
		while cursor.next(): 
			cursor.get(attrname, asof) 
	- Iterate and update data
		while cursor.next(): 
			cursor.set(attrname, asof, value)
			cursor.publish()
	- Query relationships as nested loops
		while cursor.next(): 
			linkedObjCur = cursor.link(edgename)
			while linkedObjCur.next():
				.....

