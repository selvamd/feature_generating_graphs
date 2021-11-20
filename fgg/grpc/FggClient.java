package fgg.grpc;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import fgg.utils.*;
import fgg.data.*;
import fgg.grpc.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class FggClient implements IClientAPI
{
	final ManagedChannel channel;
	final FggDataServiceGrpc.FggDataServiceStub stub;
	final FggDataServiceGrpc.FggDataServiceBlockingStub bstub;
	
	public FggClient(String host, int port) 
	{
		channel = ManagedChannelBuilder.forTarget(host+":"+port).usePlaintext(true).build();
		stub = FggDataServiceGrpc.newStub(channel);
		bstub = FggDataServiceGrpc.newBlockingStub(channel);
	}

	public boolean login(String user, String pass) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.LOGIN);
		bldr.addValues(addparam("user","test1"));	
		bldr.addValues(addparam("pass","test2"));	
		//System.out.println(bstub.queryData(bldr.build()));
		return false;
	}

	public List<Integer> getNodes() 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_NODES);
		//bldr.addValues(addparam("rootnodes",""+rootNodesOnly));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return msg.getOutkeyList();
	}
	
	public GraphItem.Node getNodeInfo(int nodeid) 
	{
		GraphItem.Node node = (GraphItem.Node) GraphItem.findByPK(nodeid);	
		if (node != null) return node;
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_NODE_INFO);
		bldr.addValues(addparam("nodekey",""+nodeid));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		
		int parent = getParamInt(msg, "parentnodekey");
		int leg    = getParamInt(msg, "legnodekey");
		int root   = getParamInt(msg, "rootnodekey");
		
		if (root == nodeid)
		{
			node = new GraphItem.RootNode(nodeid, getParam(msg, "name"));
			((GraphItem.RootNode)node).setLeg((GraphItem.Node)GraphItem.findByPK(leg));
		}
		else
		{	
			node = new GraphItem.Node(nodeid, getParam(msg, "name"));
			node.setParent((GraphItem.Node)GraphItem.findByPK(parent));
		}
		return node;
	}

    public void flush() {
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.NOTIFY_FLUSH);
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
    }
    
	public List<Integer> getEdges() 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_EDGES);
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return msg.getOutkeyList();
	}

	public GraphItem.Edge getEdgeInfo(int edgeid) 
	{
		GraphItem.Edge edge = (GraphItem.Edge)GraphItem.findByPK(edgeid);
		if (edge != null) return edge;
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_EDGE_INFO);
		bldr.addValues(addparam("edgekey",""+edgeid));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		edge = new GraphItem.Edge(edgeid, getParam(msg, "name"));
		for (int i=0;i<5;i++)
		{
			GraphItem.Node node = (GraphItem.Node)GraphItem.findByPK(getParamInt(msg, "nodekey"+i));
			if (node != null) edge.addNode(node);
		}			
		return edge;
	}

	public List<Integer> getAttrs(boolean isnode) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_ATTRS);
		bldr.addValues(addparam("isnode",""+isnode));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return msg.getOutkeyList();
	}

	public GraphItem.EdgeAttr getEdgeAttrInfo(int attrid) 
	{
		GraphItem.EdgeAttr attr = (GraphItem.EdgeAttr)GraphItem.findByPK(attrid);
		if (attr != null) return attr;
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_ATTR_INFO);
		bldr.addValues(addparam("attrkey",""+attrid));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return new GraphItem.EdgeAttr(attrid, getParam(msg, "name"), 
			(GraphItem.Edge)GraphItem.findByPK(getParamInt(msg,"key")), 
            DataType.valueOf(getParam(msg, "dtype")), getParamInt(msg, "size"));
	}
	
	public GraphItem.NodeAttr getNodeAttrInfo(int attrid) 
	{
		GraphItem.NodeAttr attr = (GraphItem.NodeAttr)GraphItem.findByPK(attrid);
		if (attr != null) return attr;
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_ATTR_INFO);
		bldr.addValues(addparam("attrkey",""+attrid));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return new GraphItem.NodeAttr(attrid, getParam(msg, "name"), 
			(GraphItem.Node)GraphItem.findByPK(getParamInt(msg,"key")), 
			DataType.valueOf(getParam(msg, "dtype")),
			getParam(msg,"splitbyleg").equals("true"),
            getParamInt(msg,"altKeyNum"), getParamInt(msg, "size"));
	}

	public int getObjPK(GraphItem.Node node, String pk) 
    {
        Map<Integer,Integer> keys = getObjKeys(node, pk, new HashMap<Integer,Integer>());
        for (int key:keys.keySet())
            if (keys.get(key) == 1)
                return key;
        return -1;
    }
    
	//This API supports elastic lookup. Key will be "objkey,altkeyseq" 
	//Only main key is returned when no matchkey arg is provided
	public Map<Integer,Integer> getObjKeys(GraphItem.Node node, String matchkey, Map<Integer,Integer> result) 
	{
        result.clear();
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_OBJ_KEYS);
		bldr.addValues(addparam("nodekey",""+node.ordinal()));
		if (matchkey == null) matchkey = "";
		bldr.addValues(addparam("matchkey",""+matchkey));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		//Map<Integer,Integer> result = new HashMap<Integer,Integer>();
		if (msg.getValuesCount()==0) return result;
        for (int i=2;i<msg.getValuesList().size();i++) 
        {
            result.put(new Integer(msg.getValuesList().get(i).getName()),
                new Integer(msg.getValuesList().get(i).getValue()));
        }
        msg.getOutkeyList().forEach((k) -> result.put(k,1));
		return result;
	}

	public int setObject(GraphItem.Node node, String key) 
	{ 
		return setObjectAltKey(node, 0, 1, key); 
	}
	
	public int setObjectAltKey(GraphItem.Node node, int objid, int altkeyseq, String key) 
	{ 
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.SET_OBJ_KEY);
		bldr.addValues(addparam("nodekey",""+node.ordinal()));
		bldr.addValues(addparam("objkey",""+objid));
		bldr.addValues(addparam("altkeyseq",""+altkeyseq));
		bldr.addValues(addparam("str_key",""+key));
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return msg.getOutkeyList().get(0);
	} 

    //returns map of linkkeys to component objectkeys for the matching pattern
	public Map<Integer,int[]> getLink2Obj(GraphItem.Edge edge, int[] objid) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_LINK_KEYS);
		bldr.addValues(addparam("edgekey",""+edge.ordinal()));
		bldr.addValues(addparam("includeobj","true"));
		for (int i=0;i<objid.length;i++)
			bldr.addValues(addparam("objkey"+i,""+objid[i]));
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());

        Map<Integer,int[]> res = new HashMap<Integer,int[]>();
        Iterator<Integer> itr = msg.getOutkeyList().iterator();
        while (itr.hasNext()) {
            int k = itr.next();
            int[] arr = new int[edge.maxnodes()];
            for (int i=0;i<arr.length;i++)
                arr[i] = itr.next();
            res.put(k,arr);
        }
		return res;
	}

    //Only returns link keys matching the pattern
	public List<Integer> getLinkKeys(GraphItem.Edge edge, int[] objid) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_LINK_KEYS);
		bldr.addValues(addparam("edgekey",""+edge.ordinal()));
		bldr.addValues(addparam("includeobj","false"));
		for (int i=0;i<objid.length;i++)
			bldr.addValues(addparam("objkey"+i,""+objid[i]));
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return msg.getOutkeyList();
	}
	
	public int setLink(GraphItem.Edge edge, int[] objid, int fromdt, int todt) 
	{ 
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.SET_LINK_KEY);
		bldr.addValues(addparam("edgekey",""+edge.ordinal()));
		for (int i=0; i < objid.length; i++)
			bldr.addValues(addparam("objkey"+i,""+objid[i]));
		bldr.addValues(addparam("fromdt",""+fromdt));
		bldr.addValues(addparam("todt",""+todt));
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
        //System.out.println(msg);
        return (msg.getOutkeyList().isEmpty())? 0:msg.getOutkeyList().get(0); 
	} 
	
	public List<Integer> getDates() 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_DATES);
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return msg.getOutkeyList();
	}

	public void getObject(GraphItem.Edge edge, int linkkey, GraphItem.Node node, int nodecnt, List<Integer> attrs, FeatureStreamListener fdo) 
        throws Exception
	{
        FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_OBJECT);
		bldr.addValues(addparam("typekey",""+node.ordinal()));
		bldr.addValues(addparam("instkey",""+linkkey));
		bldr.addValues(addparam("edgekey",""+edge.ordinal()));
		bldr.addValues(addparam("nodecnt",""+nodecnt));
        if (attrs == null) 
        {
            List<GraphItem.NodeAttr> attrkeys = GraphItem.findAttrs(node, new ArrayList<GraphItem.NodeAttr>()); 
            for (int i=0; i < attrkeys.size(); i++)
                bldr.addValues(addparam("attrkey"+i,""+attrkeys.get(i).ordinal()));
        } else {
            for (int i=0; i < attrs.size(); i++)
                bldr.addValues(addparam("attrkey"+i,""+attrs.get(i)));
        }
        requestStream(bldr.build(), fdo);
	}

	public void getLink(GraphItem.Edge edge, int linkkey, List<Integer> attrs, FeatureStreamListener fdo) 
        throws Exception
	{
        FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_OBJECT);
		bldr.addValues(addparam("typekey",""+edge.ordinal()));
		bldr.addValues(addparam("instkey",""+linkkey));
        if (attrs == null) 
        {
            List<GraphItem.EdgeAttr> attrkeys = GraphItem.findAttrs(edge, new ArrayList<GraphItem.EdgeAttr>()); 
            for (int i=0; i < attrkeys.size(); i++)
                bldr.addValues(addparam("attrkey"+i,""+attrkeys.get(i).ordinal()));
        } else {
            for (int i=0; i < attrs.size(); i++)
                bldr.addValues(addparam("attrkey"+i,""+attrs.get(i)));
        }
        requestStream(bldr.build(), fdo);
	}
    
	public void getObject(GraphItem.Node node, int objkey, List<Integer> attrs, FeatureStreamListener fdo) 
        throws Exception
	{
        FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.GET_OBJECT);
		bldr.addValues(addparam("typekey",""+node.ordinal()));
		bldr.addValues(addparam("instkey",""+objkey));
        if (attrs == null) 
        {
            List<GraphItem.NodeAttr> attrkeys = GraphItem.findAttrs(node, new ArrayList<GraphItem.NodeAttr>()); 
            for (int i=0; i < attrkeys.size(); i++)
                bldr.addValues(addparam("attrkey"+i,""+attrkeys.get(i).ordinal()));
        } else {
            for (int i=0; i < attrs.size(); i++)
                bldr.addValues(addparam("attrkey"+i,""+attrs.get(i)));
        }
        requestStream(bldr.build(), fdo);
	}
    
	public String taskRequest() 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.TASK_REQUEST);
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
		return null; //attrid,nodekey,fromdt,todt
	}

	public boolean addAttr(int nodeedgeid, String name, String datatype, String fieldtype, int size) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.ADD_ATTR);
		bldr.addValues(addparam("nodeedgeid",""+nodeedgeid));	
		bldr.addValues(addparam("attrname",name));	
		bldr.addValues(addparam("datatype",datatype));	
		bldr.addValues(addparam("fieldtype",fieldtype));	
		bldr.addValues(addparam("attrsize",""+size));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
        if (msg.getOutkeyList().size() == 0) return false;
        getNodeAttrInfo(msg.getOutkeyList().get(0));
        return true;
	}

	public void notifyGitCheckin(int attrid) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.NOTIFY_GIT_CHECKIN);
		bldr.addValues(addparam("attrid",""+attrid));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
	}

    //use refreshdt = 0  to get the current values and refreshdt > 0 to set new values
	public int notifyCBORefresh(int nodeedgeid, String attr, int dt) 
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(MsgType.NOTIFY_CBO_REFRESH);
		bldr.addValues(addparam("nodeedgeid",""+nodeedgeid));	
		bldr.addValues(addparam("attrname",attr));	
		bldr.addValues(addparam("refreshdt",""+dt));	
		FggDataServiceOuterClass.FggMsg msg = bstub.queryData(bldr.build());
        return msg.getOutkeyList().get(0);
	}
	
	private static StreamObserver<FggDataServiceOuterClass.FggMsg> createAckReceiver(final AtomicInteger count) 
	{
		return new StreamObserver<FggDataServiceOuterClass.FggMsg>() 
		{
			@Override
			public void onNext(FggDataServiceOuterClass.FggMsg msg) 
			{
				fgg.grpc.FggDataServiceOuterClass.params p = msg.getValuesList().get(0);
				if (msg.getRequest() == MsgType.STATUS.ordinal()) {
					count.getAndAdd(Integer.parseInt(p.getValue()));
				} else {
					System.out.println("Server error " + p.getValue());
					count.set(-1);
				}
			}

			@Override
			public void onError(Throwable t) {
				System.out.println("Ack receiver error");
				count.set(-1);
			}

			@Override
			public void onCompleted() {
			}
		};
	}
	
	//Sends block of requests to the server and waits for response. 
    //Returns number of msgs published or -1 for error 
	public int publish(final Collection<FeatureData> datalist) throws Exception 
	{
		AtomicInteger req = new AtomicInteger(0);
		publishNonBlocking(datalist,req);
		while (req.get() == 0)
			Thread.sleep(10);
		return req.get();
	}

	//Sends block of requests to the server and returns without blocking. 
	//Poll the return value till it becomes non-zero to check for completion. 
    //Returns number of msgs published or -1 for error
    private int requestsize = 0;
	public AtomicInteger publishNonBlocking(final Collection<FeatureData> datalist, AtomicInteger requests) throws Exception 
	{
        //throttles the requests
        requestsize += datalist.size();
        while (requestsize - requests.get() > 20000) 
            Thread.sleep(10);
        
		StreamObserver<FggDataServiceOuterClass.FggData> requestObserver = stub.persistData(createAckReceiver(requests));
		for (FeatureData data:datalist)
			for (FggDataServiceOuterClass.FggData d:data.xmits()) {
				requestObserver.onNext(d);
            }
		requestObserver.onCompleted();
		return requests;
	}
	
	//Send single request and get single response
    public FggDataServiceOuterClass.FggMsg query(MsgType type) throws Exception
	{
		FggDataServiceOuterClass.FggMsg request = 
			FggDataServiceOuterClass.FggMsg.newBuilder().setRequest(type.ordinal()).build();
		return bstub.queryData(request);
	}
	
	//Send single request and get stream of responses
    public FeatureStreamListener requestStream(FggDataServiceOuterClass.FggMsg req, FeatureStreamListener fdo) throws Exception
    {
		stub.requestData(req, fdo.getReceiver());
        return fdo;
    }

	public void connect() 
	{
		login("user1","pass1");

		//Initialize single nodes
		for (int nodeid:getNodes()) 
			getNodeInfo(nodeid);

		//Initialize edges
		for (int edgeid:getEdges()) 
			getEdgeInfo(edgeid);
		
		//Initialize node attrs
		for (int attrid:getAttrs(true))
			getNodeAttrInfo(attrid);

		//Initialize edge attrs
		for (int attrid:getAttrs(false))
			getEdgeAttrInfo(attrid);
	}
	
    public static void main( String[] args ) 
        throws Exception
	{
		FggClient c = new FggClient("localhost",33789);
		c.connect();

        GraphItem.Node cust = (GraphItem.Node)GraphItem.findByName("Customer");
        int key = c.getObjPK(cust, "1000359037");
        //FeatureStreamListener callback = c.getObject(cust, key, null);
        //List<FeatureData> data = callback.get();
        //while ((data = callback.get()) == null)
        //    Thread.sleep(10);
        //System.out.println("Feature Data size = " + data.size());
		//GraphItem.Node node = (GraphItem.Node) GraphItem.findByName(args[0]);
		//List<GraphItem.NodeAttr> list = GraphItem.findAttrs(node, new ArrayList<GraphItem.NodeAttr>());
		//list.forEach(x -> System.out.println(x));
        
        //Server stores it and so request it back
        
        

		
		/*
		FeatureData f = new FeatureData(GraphItem.findNodeAttr(0));
		f.addLinkNodeKey(GraphItem.findNode(0), 100);
		f.addLinkNodeKey(GraphItem.findNode(1), 120);
		f.seto(20150101, new Integer(100));
		f.seto(20190101, new Integer(120));
		
		try {
			for (int i=0;i<2;i++)
				System.out.println(c.query(MsgType.LOGIN));
			//Publish this 10 times
			List<FeatureData> data = new ArrayList<FeatureData>();
			for (int i=0; i<20; i++)
				data.add(f);
			
			c.publish(data); //use non-blocking version to sync
			
			FeatureStreamListener fdo = new FeatureStreamListener() {
				public void onNext(FeatureData data) {
					System.out.println(data);
				}
			};
			
			//Server stores it and so request it back
			c.requestStream(MsgType.LOGIN, fdo);
			
			while (!fdo.isComplete())
				Thread.sleep(1000);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		*/
	}

	/////////////////////////////////////// HELPERS ///////////////////////////////////////////
	private static FggDataServiceOuterClass.params addparam(String name, String val) {
		return FggDataServiceOuterClass.params.newBuilder().setName(name).setValue(val).build();
	}

	private static int getParamInt(FggDataServiceOuterClass.FggMsg request, String paramname) {
		String val = getParam(request,paramname);
		return (val==null||val.length()==0)? -1:Integer.parseInt(val);
	}
	
	private static String getParam(FggDataServiceOuterClass.FggMsg request, String paramname) {
		String paramid = null;
		for (FggDataServiceOuterClass.params param:request.getValuesList())
			if (param.getName().equals(paramname))
				paramid = param.getValue();
		return paramid;
	}		
	
	private static FggDataServiceOuterClass.FggMsg.Builder create(MsgType type) {
		return FggDataServiceOuterClass.FggMsg.newBuilder().setRequest(type.ordinal());
	}		
	
}
