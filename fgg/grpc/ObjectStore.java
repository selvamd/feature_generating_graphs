package fgg.grpc;
import fgg.data.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public interface ObjectStore
{
    public static ObjectStore make() { return new ObjectStoreImpl(); }

    public AtomicInteger publishNonBlocking(Collection<FeatureData> data, AtomicInteger res) throws Exception;
        
    //Returns all the object names in the store
    public String[] getObjectNames();

    //Returns all the attr names for the objects in the store
    public String[] getAttrNames(String objectName);

    //Returns dtype of a single object attribute
    public String getDataType(String objectName,String attrName);

    //Returns data size of a single object attribute
    public int getDataSize(String objectName,String attrName);

    public void sendFlush();

    //Runs a query on single object collection and returns a cursor to iterate all the objects
    public ObjectCursor query(String objectName, String selects, String filter);

    //typekey = node or edge key
	public void addAttr(int typekey, String name, DataType dtype, FieldType ftype, int size);

    public int getObjectPk(String objectName, String key);
    
    //returns just linkkeys
	public int[] getLinks(GraphItem.Edge edge, int[] objid);

    //returns map of linkkeys -> objkeys[]
    public Map<Integer,int[]> getLink2Obj(GraphItem.Edge edge, int[] objid);
    
	public int setObject(String objectName, int objpk, String key); 
    
	public int setObjectAltKey(String objectName, int objid, int keyseq, String key);

	public int setLink(GraphItem.Edge edge, int[] objid, int fromdt, int todt);
    
    public void printSchema();

}
