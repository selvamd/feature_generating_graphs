package fgg.grpc;

import fgg.utils.*;
import fgg.access.*;
import fgg.data.*;
import java.util.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class FggSetupMetaData
{
    static List<CBO> nodes = new ArrayList<CBO>();
    static List<CBO> nodetrees = new ArrayList<CBO>();
    static Map<String,CBO> attrs = new TreeMap<String,CBO>();
    static List<CBO> edges = new ArrayList<CBO>();

    public static void main( String[] args ) throws Exception
    {
        CBOBuilder.buildMeta();
        CBOBuilder.buildMaxKeys();
        CBOBuilder.buildCBOTypes();
        CBOBuilder.buildLinkTypes();
        CBOBuilder.buildAttrs();

        // Create a dom from the xml.
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.parse("./conf/dsl.xml");
        Element rootEle = (Element) doc.getDocumentElement();

        Map<String,Integer> map = createNodes(rootEle);
        CBOBuilder.persist(CBOBuilder.Table.NODE, nodes);
        CBOBuilder.persist(CBOBuilder.Table.NODE_TREE, nodetrees);
        CBOBuilder.buildCBOTypes();

        createEdges(rootEle, map);
		CBOBuilder.persist(CBOBuilder.Table.EDGE, edges);
        CBOBuilder.buildLinkTypes();

        createAttr(rootEle);
        createEdgeAttr();
		CBOBuilder.persist(CBOBuilder.Table.ATTR, attrs.values());
        CBOBuilder.buildAttrs();
		makenodekeys(rootEle);
    }

	public static void makenodekey(String node, String attrstr, int seqnum)
	{
        CBOType type = CBOType.valueOf(node);
		Set<Integer> set = FieldMeta.getAttrKeys(type, new TreeSet<Integer>());
		set.removeIf(attrkey -> attrstr.toLowerCase().indexOf(FieldMeta.lookup(attrkey).fname()) == -1);
		CBOBuilder.makeAltKey(type.ordinal(), seqnum, (SortedSet<Integer>)set);
	}

	public static void makenodekeys(Element root)
    {
        NodeList list = root.getElementsByTagName("object");
        for (int i = 0; i < list.getLength();i++)
        {
            Element ele = (Element)list.item(i);
            NodeList keylist = ele.getElementsByTagName("keys");
            for (int j = 0; j < keylist.getLength(); j++) {
                String keys = keylist.item(j).getChildNodes().item(0).getNodeValue();
                makenodekey(toggle(ele.getAttribute("name")), keys, j+1);
            }
        }
	}

    public static int lookupOrCreateRootNode(String name, int leg)
    {
        CBO rec = null;
        CBOType type = CBOType.valueOf(name);
        if (type != null) return type.ordinal();
        nodes.add(rec = CBOBuilder.createRootNode(name, leg));
        return rec.recid();
    }

    public static int lookupOrCreateNode(String name, int parent, int root)
    {
        CBO rec = null;
        CBOType type = CBOType.valueOf(name);
        if (type != null) return type.ordinal();
        nodes.add(rec = CBOBuilder.createNode(name, parent, root));
        nodetrees.add(CBOBuilder.createNodeTree(parent, rec.recid()));
        return rec.recid();
    }

    public static void createEdges(Element root, Map<String,Integer> map)
    {
        NodeList list = root.getElementsByTagName("link");
        for (int i = 0; i < list.getLength();i++)
        {
            Element ele = (Element)list.item(i);
            String name = ele.getAttribute("name");
            if (name.trim().length() == 0) name = null;
            String[] objects = ele.getAttribute("objects").split(",");
            lookupOrCreateEdge(toggle(objects[0]), toggle(objects[1]), name);
        }
	}

	public static int lookupOrCreateEdge(String parentcbo, String childcbo, String edge)
	{
		CBOType p = CBOType.valueOf(parentcbo);
		CBOType c = CBOType.valueOf(childcbo);
		if (p == null || c == null) return -1;
        boolean isDefault = (edge == null);
		if (edge == null) edge = p.ordinal()+"_"+c.ordinal();
		LinkType type = LinkType.valueOf(edge);
		if (type != null) return type.ordinal();
		CBO cbo = CBOBuilder.createEdge(edge, isDefault, new int[] { p.ordinal(), c.ordinal() }, new int[] { 20, 11 });
		edges.add(cbo);
		return cbo.recid();
	}

    public static String toggle(String str) {
        if (str == null) return str;
        return str.substring(0,1).toUpperCase() +
                str.substring(1).toLowerCase();
    }

    public static Map<String,Integer> createNodes(Element root)
    {
        Map<String,Integer> map = new LinkedHashMap<String,Integer>();
        NodeList list = root.getElementsByTagName("object");
        for (int i = 0; i < list.getLength();i++)
        {
            Element ele = (Element)list.item(i);
            if (ele.getAttribute("leg") != null && ele.getAttribute("leg").length() > 0) continue;
            map.put(ele.getAttribute("name"),
                lookupOrCreateRootNode(toggle(ele.getAttribute("name")), 0));
        }
        for (int i = 0; i < list.getLength();i++)
        {
            Element ele = (Element)list.item(i);
            Integer leg = map.get(ele.getAttribute("leg"));
            if (leg == null) continue;
            map.put(ele.getAttribute("name"),
                lookupOrCreateRootNode(toggle(ele.getAttribute("name")), leg));
        }
        return map;
    }

    public static Map<String,String> getAttrProperties(boolean isenum) { return CBOBuilder.getAttrProperties(isenum); }

    public static void createOrLookupNodeAttr(String ctype, String name, DataType dtype, FieldType ftype, int maxsize, boolean isenum)
    {
		name = name.toLowerCase();
		CBOType cbtype = CBOType.valueOf(ctype);
		Map<String,String> prop = getAttrProperties(isenum);
        FieldMeta m = FieldMeta.lookup(cbtype, name);
        if (m != null) return;
        attrs.put(name,CBOBuilder.createAttr(cbtype, name, dtype, ftype, maxsize, prop));
    }

    public static void createOrLookupEdgeAttr(LinkType type, String name, DataType dtype, FieldType ftype, int maxsize, boolean isenum)
    {
		name = name.toLowerCase();
		Map<String,String> prop = getAttrProperties(isenum);
        FieldMeta m = FieldMeta.lookup(type, name);
        if (m != null) return;
        attrs.put(type+"."+name,CBOBuilder.createAttr(type, name, dtype, ftype, maxsize, prop));
    }

    public static void createEdgeAttr()
    {
        for (LinkType type: LinkType.values()) {
            for (int i=0;i<type.maxnodes();i++) {
                CBOType c = CBOType.valueOf(type.nodekey(i));
                createOrLookupEdgeAttr(type, c.name().toLowerCase() +"_id", DataType.INT, FieldType.CORE, 4, false);
            }
            createOrLookupEdgeAttr(type, "isvalid", DataType.INT, FieldType.CORE, 4, false);
        }
    }

    public static void createAttr(Element root) throws Exception {
        NodeList list = root.getElementsByTagName("object");
        for (int i = 0; i < list.getLength();i++)
        {
            Element ele = (Element)list.item(i);
            NodeList fldlist = ele.getElementsByTagName("field-name");
            for (int j = 0; j < fldlist.getLength(); j++)
            {
                Element fldEle = (Element)fldlist.item(j);
                String field = fldEle.getChildNodes().item(0).getNodeValue();
                DataType type = DataType.valueOf(fldEle.getAttribute("type"));
                if (type == null) throw new Exception ("Invalid data type " + fldEle.getAttribute("type"));
                createOrLookupNodeAttr(ele.getAttribute("name"), field, type, FieldType.CORE,
                    Integer.parseInt(fldEle.getAttribute("size")), (type == DataType.ENUM));
            }
        }
    }
}
