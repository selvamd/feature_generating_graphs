package fgg.grpc;

import java.util.*;
import fgg.data.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectStoreImpl implements ObjectStore
{
    private FggClient client;
    
    public ObjectStoreImpl() 
    {
        this.client = new FggClient("localhost",33789);
        this.client.connect();
        System.out.println("ObjectStore connected");
    }
    
    public AtomicInteger publishNonBlocking(Collection<FeatureData> data, AtomicInteger res) throws Exception {
        return client.publishNonBlocking(data, res);
    }

    //Returns all the object names in the store
    public String[] getObjectNames() 
    {
        List<Integer> nodes = client.getNodes();
        String[] res = new String[nodes.size()];
        for (int i=0;i<res.length;i++) 
        {
            GraphItem.Node info = client.getNodeInfo(nodes.get(i));
            res[i] = info.name();
        }
        return res;
    }

    //Returns all the attr names for the objects in the store
    public String[] getAttrNames(String objectName) {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return null;
        List<GraphItem.NodeAttr> attrs = 
            GraphItem.findAttrs(info, new ArrayList<GraphItem.NodeAttr>());
        String[] res = new String[attrs.size()];
        for (int i=0;i<res.length;i++) 
            res[i] = attrs.get(i).name();
        return res;
    }

	public void addAttr(int typekey, String name, DataType dtype, FieldType ftype, int size) {
        client.addAttr(typekey, name, dtype.toString(), ftype.toString(), size);
    }
    
    //Returns dtype of a single object attribute
    public String getDataType(String objectName,String attrName) {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return null;
        GraphItem.NodeAttr attr = GraphItem.findAttr(info, attrName);
        if (attr == null) return null;
        return attr.dtype().toString();
    }

    //Returns dtype of a single object attribute
    public int getDataSize(String objectName,String attrName) {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return -1;
        GraphItem.NodeAttr attr = GraphItem.findAttr(info, attrName);
        if (attr == null) return -1;
        return attr.size();
    }

    public int getObjectPk(String objectName, String key) 
    {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return -1;
        return client.getObjPK(info, key);
    }

    public int[] getLinks(GraphItem.Edge edge, int[] objid) 
    {
        List<Integer> lkeys = client.getLinkKeys(edge, objid);
        int[] res = new int[lkeys.size()];
        for (int i=0;i<res.length;i++) 
            res[i] = lkeys.get(i);
        return res;
    }

    public Map<Integer,int[]> getLink2Obj(GraphItem.Edge edge, int[] objid) {
        return client.getLink2Obj(edge, objid);
    }
    
	public int setObject(String objectName, int objpk, String key) 
    {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return -1;
        return client.setObjectAltKey(info, objpk, 1, key);
        //return client.setObject(info, key);
    }        
    
	public int setObjectAltKey(String objectName, int objid, int keyseq, String key)
    {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return -1;
        return client.setObjectAltKey(info, objid, keyseq, key);
    }        
    
    public ObjectCursor query(String objectName, String selects, String filter) 
    {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return null;
        Map<Integer,Integer> map = client.getObjKeys(info, null, new HashMap<Integer,Integer>());
        int[] result = new int[map.size()];
        int index = 0;
        for (int key:map.keySet())
            result[index++] = key;
        
        if (selects == null) 
            for (String attr:getAttrNames(objectName))
                selects = (selects == null)? attr:selects+","+attr;
            
        ObjectCursor cur = new ObjectCursorImpl(client, info, result);
        return (cur.selectAttrs(selects))? cur:null;
    }

	public int setLink(GraphItem.Edge edge, int[] objid, int fromdt, int todt) {
        return client.setLink(edge, objid, fromdt, todt);
    }

    public void sendFlush() {
        client.flush();
    }

    public void printSchema() {
        String[] objects = getObjectNames();
        for (String obj:objects) {
            System.out.println(obj);
            String[] attrs = getAttrNames(obj);
            for (String attr:attrs)
                System.out.println("\t"+attr + "\t" + getDataType(obj,attr));
        }
        System.out.println("");
    }

}
