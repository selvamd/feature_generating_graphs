# What is FCG? 
FGG (feature_generating_graphs) is a read-write object graph framework for data virtualization, dynamic read-writes, traversals and feature computations

## Feature Engineering
 - Refers to data enrichments that happen in the context of model development
 - The least automatable part of MDLC and the most time intensive. Highly contextual to the model created and based on domain data. Gives less opportunity to automate using third party tools
 - Functionally analogous to “T” in ETL but on steroids. Traditional ETL transforms (Rollup, Drilldown, Slicing, Dicing) can generate some features but not all. Models need features generated from statistical, timeseries, sampling, simulation and (other) model-based techniques. Companies like snowflake try to overcome the limitations by creating programming ecosystem with SQL. But SQL as a technology doesnot seem to be the natural ecosystem for a comprehensive solution. 
 - Requires experimentation, tuning, tweaking and validation to ascertain its correctness and fit for the model. Requires tight integration with visualization and analysis tools 
 - Features can be inferential derived from business assumptions. Makes the computational results less stable than factually based ones. Storage pipelines need to support recomputes and retroactive back-propagation
 - Cost of feature creation is not always equal. Features may require dynamic generation to optimize for storage and may need to be pre-generated (aka memorized) to optimize for speed and computional costs.
 - To scale ML efforts for large teams, (a) features need to be sharable across teams/projects/models, (b) precise definitions, as documentation or code, needs to be discoverable.
 - Analysts need a self-service setup to productionize features. The tight coupling makes a factory type of ownership handoff to data engineers very inefficient

## ML Stack on Graph
- FCG is a graph DB designed for feature engineering. It is designed with a combination of functions that makes it very powerful tool that can support model development.
	- Business objects as nodes. Properties and edges are timeseries
 	- Supports real time read/write with ability to broadcast state changes
  	- State changes applied as curational or mutational at retrieval time
  	- Enforces additional structure on top of native graph data model
  		- Logical data model based on predefined objects and relationships
  	 	- Nothing anonymous. All nodes and edges adhere to strict type system
  	  	- Cardinality rules enforced on relationships
  	  	- Potential to support hyper-graphs in the model
	- Ability to export to multi-model DB formats
 		- Relational, Graph and hierarchical (Document/JSON/BSON)
   	- Decorator interfaces during retrieval for dynamic curation.
   		- Extrapolate, Interpolate, obfuscate, mask or gap fill

 - Useful as unified stack for real time feature engineering and warehousing process
 - Generate horizontally scalable big tables thru updates from disparate asynchronous pipelines
 - Provide unified consistent schema for historical and current state of objects
 - Support full and incremental import/export capabilities
 - Ability to virtualize data as in-memory cached objects with CRUD abilities
 - Data abstraction to serve both dynamic or memoized features
 - Dynamic aggregations or reduction can happen in horizontal and/or vertical axis
 - Complimentary to any modern Analytics platform - Alterix, Knime, H20.ai, Databricks, IBM Watson, AWS Sagemaker

## FGG is a structured graph 
- Node is the prototype for objects (or conversely objects are instances of a node) 
- Edges are defined as connection between a set of nodes and therefore are prototypes for relationships.
- Analogous to objects for node, link refers to an instance of an edge (or a relationship between a set of objects)
- Edges connecting more than 2 nodes are possible and creates what are called as Hypergraphs
Attributes can be added to both nodes and edges and its instances are timeseries objects and not scalar values  

## Uses GRPC to support multi-language clients
FGG publishes language specific client library (currently available for python and java) that abstracts the low level connectivity details to the remote object store. This section describes (in pseudocode) how to interact with the graph. For a working example refer to fgg/pyclients/testclient.py 

## Basic interactions with object graph 
	- Create an instance of FggStore (Clientside stub handling all server interactions)
		store = FggStore()
	- Create a Object of a particular node
		objid = store.setObject(nodename, Key)
	- Setup relationship between 2 objects
		store.setLink(edgename,[obj1.id,obj2.id], fromdt, todt)
	- Define new Attribute for a node or an edge 
		store.addAttr(node_edge_name, attrname, datatype..)
	- Obtain cursor to browse objects in object store
		cursor = store.query(nodename, attributes, filter, sort)
	- Iterate and read data
		while cursor.next(): 
			cursor.get(attrname, asof) 
	- Iterate and update data
		while cursor.next(): 
			cursor.set(attrname, asof, value)
			cursor.publish()
	- Navigate the graph by traversing thru the relationships
		while cursor.next(): 
			linkedObjCur = cursor.link(edgename)
			while linkedObjCur.next():
				.....


   
