package fgg.grpc;

import fgg.grpc.*;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import fgg.utils.*;
import fgg.data.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public interface IClientAPI
{
	public boolean login(String user, String pass);

	/////// APIs for schema/metadata download ///////
	//public List<Integer> getNodes();
	//public GraphItem.Node getNodeInfo(int nodeid);
	//public List<Integer> getEdges();
	//public GraphItem.Edge getEdgeInfo(int edgeid);
	//public List<Integer> getAttrs(boolean isnode);
	//public GraphItem.EdgeAttr getEdgeAttrInfo(int attrid);
	//public GraphItem.NodeAttr getNodeAttrInfo(int attrid);
	public void connect(); // Invokes all to re-build schema locally
	public List<Integer> getDates();

	//////////// Helper methods from downloaded schema /////////////////////////
	//public static GraphItem.Node findNode(String name) { return (GraphItem.Node)GraphItem.findByName(name); }
	//public static List<GraphItem.Edge> findEdges(String nodenames) { return GraphItem.findEdges(nodenames); }

	/////////////////// Update APIs for setting objkeys and linkkeys //////////////////////////////////////////////////////
    //lookup or create object with external key #1 (altkeyseq=1)
	public int setObject(GraphItem.Node node, String key); 
	public int setObjectAltKey(GraphItem.Node node, int objid, int altkeyseq, String key);
	public int setLink(GraphItem.Edge edge, int[] objid, int fromdt, int todt);

	//Sends a block of requests to the server and waits for the response.
	//Returns number of msgs published or -1 for error
	public int publish(final Collection<FeatureData> datalist) throws Exception;
	public void flush() throws Exception;

	//Sends a block of requests to the server and returns without blocking.
	//Poll the return value till it becomes non-zero to check for completion.
	//Returns number of msgs published or -1 for error
	public AtomicInteger publishNonBlocking(final Collection<FeatureData> datalist, AtomicInteger req) throws Exception;

	/////////////////////////////// Read APIs for obtaining objkeys and linkkeys //////////////////////////////////////////

	//This API supports elastic lookup on primary and secondary strkeys. Returns "objkey,altkeyseq".
	//If matchkey = null, all of the main keys are returned as "objkey,1"
	public Map<Integer,Integer> getObjKeys(GraphItem.Node node, String matchkey, Map<Integer,Integer> result);

	//Convenience method using the above to provide lookup of objkey from the primary strkey
	public int getObjPK(GraphItem.Node node, String strkey);

	//Returns linkkeys. Performs partial matches ignoring any objid[idx] set to 0.
	public List<Integer> getLinkKeys(GraphItem.Edge edge, int[] objid, int asofdt);
    public Map<Integer,int[]> getLink2Obj(GraphItem.Edge edge, int[] objid);
    
	//Returns FeatureStreamListener to return node data for given a objkey.
	public void getObject(GraphItem.Node node, int objkey, List<Integer> attrs, FeatureStreamListener listener)
	throws Exception;
    
	//Returns FeatureStreamListener to return node data based on linkkey
    //nodecnt = Serves as position identifier in case of edges between same nodes
	public void getObject(GraphItem.Edge edge, int linkkey, GraphItem.Node node, int nodecnt, List<Integer> attrs, FeatureStreamListener fdo) 
	throws Exception;

	//Returns FeatureStreamListener to return edge data based on linkkey 
	public void getLink(GraphItem.Edge edge, int linkkey, List<Integer> attrs, FeatureStreamListener fdo) 
        throws Exception;

	public String taskRequest();
    
    //fieldType=CORE,STATIC,DYNAMIC,VIRTUAL
    //datatype=BYTE,CHAR,SHORT,INT,LONG,FLOAT,DOUBLE,STRING,DATE,KEY,ENUM
	public boolean addAttr(int nodeedgeid, String name, String datatype, String fieldtype, int size);
    
	public void notifyGitCheckin(int attrid);
    
    //use dt = 0  to get the current values and dt > 0 to set new values
	public int notifyCBORefresh(int nodeedgeid, String attr, int dt);
}
