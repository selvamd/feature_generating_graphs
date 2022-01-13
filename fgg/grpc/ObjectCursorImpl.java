package fgg.grpc;
import java.util.*;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ObjectCursorImpl implements ObjectCursor
{
    private FggClient client;
    private GraphItem.Node node;

    private GraphItem.Edge edge; //null for root cursors
    private int nodecnt;
    //private String    filter;

    //Contains objkeys for root-cursors, linkkeys for linked-cursors
    private int[] result; //Set of keys to iterate upon
    private int idx = 0;

    private List<FeatureData> features;
    private List<Integer> attrs;
    private List<Integer> edgeattrs;

    //Constructor for root-cursor
    ObjectCursorImpl(FggClient c, GraphItem.Node n, int[] keys)
    {
        client   = c;
        node     = n;
        nodecnt  = 0;
        result   = keys;
        idx      = -1;
        features = new ArrayList<FeatureData>();
        attrs    = new ArrayList<Integer>();
        edgeattrs = new ArrayList<Integer>();
        //System.out.println("Cursor size = " + keys.length);
        //for (int key:keys) System.out.print(key + "\t");
        //System.out.println("");
    }

    //Constructor for linked cursor
    ObjectCursorImpl(FggClient c, GraphItem.Edge e, GraphItem.Node n, int cnt, int[] keys)
    {
        client = c;
        node   = n;
        nodecnt  = cnt;
        edge   = e;
        result = keys;
        idx    = -1;
        features = new ArrayList<FeatureData>();
        attrs    = new ArrayList<Integer>();
        edgeattrs = new ArrayList<Integer>();
        //System.out.println("Cursor size = " + keys.length);
        //for (int key:keys) System.out.print(key + "\t");
        //System.out.println("");
    }

    public Set<Integer> keys(Set<Integer> set)
    {
        set.clear();
        if (result == null) return set;
        for (int key:result) set.add(key);
        return set;
    }

    //Returns a cursor to the related objects
    //This can be recursively called to navigate the object tree
    public ObjectCursor linkByName(String linkName, int asofdt)
    {
        GraphItem.Edge edge = GraphItem.findEdge(linkName);
        if (edge == null) return null;
        GraphItem.Node info = edge.node((edge.index(node)+1)%2);

        int[] objid = new int[2];
        objid[edge.index(node)] = result[idx];

        List<Integer> fks = client.getLinkKeys(edge, objid, asofdt);
        int[] result = new int[fks.size()];
        int index = 0;
        for (int key:fks)
            result[index++] = key;
        int cnt = (info.equals(node))? 1:0;
        return new ObjectCursorImpl(client, edge, info, cnt, result);
    }

    //Returns a cursor to the related objects
    //This can be recursively called to navigate the object tree
    public ObjectCursor link(String objectName, int asofdt)
    {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return null;
        GraphItem.Edge edge = GraphItem.findDefaultEdge(new GraphItem.Node[] { info, node });
        if (edge == null) return null;

        int[] objid = new int[2];
        objid[edge.index(node)] = result[idx];

        List<Integer> fks = client.getLinkKeys(edge, objid, asofdt);
        int[] result = new int[fks.size()];
        int index = 0;
        for (int key:fks)
            result[index++] = key;
        int cnt = (info.equals(node))? 1:0;
        return new ObjectCursorImpl(client, edge, info, cnt, result);
    }

    public int size() {
        return (result == null)? 0:result.length;
    }
	
    /*
    public void addLinkByName(String linkName, int objkey, int fromdt, int todt)
    {
        GraphItem.Edge edge = GraphItem.findEdge(linkName);
        if (edge == null || todt < fromdt) return;
        int[] objid = new int[2];
        objid[edge.index(node)] = result[idx];
        objid[(edge.index(node)+1)%2] = objkey;
        client.setLink(edge, objid, fromdt, todt);
    }

    public void addLink(String objectName, int objkey, int fromdt, int todt)
    {
        GraphItem.Node info = GraphItem.findNode(objectName);
        if (info == null) return;
        GraphItem.Edge edge = GraphItem.findDefaultEdge(new GraphItem.Node[] { info, node });
        if (edge == null || todt < fromdt) return;
        int[] objid = new int[2];
        objid[edge.index(node)] = result[idx];
        objid[edge.index(info)] = objkey;
        client.setLink(edge, objid, fromdt, todt);
    } */

    private void fetch()
    {
        features.clear();
        final int[] status = new int[] {0};
        FeatureStreamListener fdo = new FeatureStreamListener() {
            public void onNext(FeatureData data) {}
            public void onComplete(List<FeatureData> data) {
                features.addAll(data);
                status[0] = 1;
            }
        };
        try {
            if (idx >= 0 && idx < result.length)
            {
                if (edge != null) {
                    if (attrs.size() > 0)
                        client.getObject(edge, result[idx], node, nodecnt, attrs, fdo);
                    if (edgeattrs.size() > 0)
                        client.getLink(edge, result[idx], edgeattrs, fdo);
                } else
                    client.getObject(node, result[idx], attrs, fdo);
                while (status[0] == 0) Thread.sleep(1);
            }
            //System.out.println(features);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //Each call to next moves the cursor to the next object
    public boolean next() {
        ++idx;
        fetch();
        return (idx < result.length);
    }

    //Adds new Attr from node (for root-cursor) or
    //linktarget (for linked-cursor) to query list
    public boolean selectAttrs(String attrlist)
    {
        for (String attr:attrlist.split(","))
        {
            GraphItem.NodeAttr atg = GraphItem.findAttr(node, attr);
            if (atg == null) return false;
            attrs.add(atg.ordinal());
        }
        fetch();
        return true;
    }

    public List<String> attrs() {
        List<String> attrlist = new ArrayList<String>();
        for (int iattr:attrs) {
            GraphItem.NodeAttr atg = (GraphItem.NodeAttr)GraphItem.findByPK(iattr);
            attrlist.add(atg.name());
        }
        return attrlist;
    }

    public boolean selectLinkAttrs(String attrlist)
    {
        for (String attr:attrlist.split(","))
        {
            GraphItem.EdgeAttr atg = GraphItem.findAttr(edge, attr);
            if (atg == null) return false;
            edgeattrs.add(atg.ordinal());
        }
        fetch();
        return true;
    }

    public int getObjectPk() {
        if (edge != null)
            throw new RuntimeException("Invalid for linkedcursors");
        return result[idx];
    }

    public int getLinkPk() {
        if (edge == null)
            throw new RuntimeException("Invalid for rootcursors");
        return result[idx];
    }


    //Retrieves attr value for the current object
    public String get(String attr, int asofdt)
    {
        for (FeatureData data:features)
        {
            if (!data.getField().name().equals(attr)) continue;
            //if (attr.equals("account_number")) {
            //    System.out.println("----------");
            //    System.out.println(data);
            //}
            //if (attr.equals("acct_key")||attr.equals("date_key"))
            //    System.out.println(data);
            return ""+data.geto(asofdt);
        }
        return null;
    }

    public SortedSet<Integer> getScdDates(SortedSet<Integer> out)
    {
        out.clear();
        for (FeatureData data:features)
        {
            //Skip any key fields as they are always initialized with mindate (20150101)
            GraphItem.NodeAttr attr = (GraphItem.NodeAttr) data.getField();
            //System.out.println(attr.name() + ":" + attr.altKeyNum());
            if (attr.altKeyNum() > 0) continue;
            out.addAll(data.scddates());
        }
        return out;
    }

    //Updates attr value for the current object locally
    public void set(String attr, int asofdt, String value)
    {
        for (FeatureData data:features)
        {
            if (!data.getField().name().equals(attr)) continue;
            data.seto(asofdt, value);
        }
    }

    //Publish the values to the server
    public void publish() {
        try {
            //System.out.println(features);
            client.publishNonBlocking(features,new AtomicInteger(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void hasChangedOn(int asofdt) {
        Set<Integer> set = new HashSet<Integer>();
        for (FeatureData data:features) {
            set.clear();
            set.addAll(data.scddates());
            if (set.contains(asofdt))
                System.out.println(data.getField().ordinal()+"="+data.getField().name());
        }
    }
}
