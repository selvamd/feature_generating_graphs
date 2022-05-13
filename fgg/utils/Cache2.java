package fgg.utils;

import fgg.access.*;
import fgg.access.Persistor;
import fgg.data.*;
import java.util.*;
import java.util.stream.*;

public class Cache2
{
	private static SortedSet<Integer> dates;
	private static Map<LinkType,EdgeDB> edges;
	private static Map<CBOType,NodeKeyDB> nodes;
	private static Map<CBOType,List<NodeDataDB>> nodevalues;
	private static Map<LinkType,List<EdgeDataDB>> edgevalues;

	public static void init()
    {
        edges = new HashMap<LinkType,EdgeDB>();
        nodes = new HashMap<CBOType,NodeKeyDB>();
        nodevalues = new HashMap<CBOType,List<NodeDataDB>>();
        edgevalues = new HashMap<LinkType,List<EdgeDataDB>>();
        Runtime.getRuntime().addShutdownHook(
            new Thread() {
                public void run()
                {
                    Cache2.flush();
                }
            });
		try {
            //Logger.enable(Logger.ENUM_GROUP);
            EnumGroup.load();
            CBOBuilder.buildAll();
            buildDB();
			buildDates();
		} catch (Exception e) {
			System.out.println("Cache Initialization failed");
			e.printStackTrace();
		}
	}

	private static void buildDB() throws Exception
    {
		for (CBOType type:CBOType.values())
		{
			if (!type.isRoot()) continue;
            nodes.put(type,new NodeKeyDB(type));
            List<NodeDataDB> list = null;
            nodevalues.put(type,list = new ArrayList<NodeDataDB>());
            list.add(new NodeDataDB(type,0,1));
		}

		for (LinkType ltype:LinkType.values())
        {
            edges.put(ltype,new EdgeDB(ltype));
            List<EdgeDataDB> list = null;
            edgevalues.put(ltype,list = new ArrayList<EdgeDataDB>());
            list.add(new EdgeDataDB(ltype,0,1));
        }
    }

	private static void buildDates() throws Exception
	{
		//////////////////////// Initialized valid row dates //////////////////
		dates = new TreeSet<Integer>();
		int dt = Utils.mindate(), today = Integer.parseInt(Persistor.today().replaceAll("-", ""));
		while (dt < today)
        {
            dates.add(dt);
            dt = Persistor.addDate(dt,1);
		}
		System.out.println("Loaded valid history dates");
	}

	public static void flush()
    {
        try {
            EnumGroup.flush();
            for (EdgeDB db:edges.values())
                db.flush();
            for (NodeKeyDB db:nodes.values())
                db.flush();
            for (List<NodeDataDB> dbs:nodevalues.values())
                for (NodeDataDB db:dbs)
                    db.flush();
            for (List<EdgeDataDB> dbs:edgevalues.values())
                for (EdgeDataDB db:dbs)
                    db.flush();
            System.out.println("All object caches flushed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

	public static SortedSet<Integer> month_dates()
    {
		SortedSet<Integer> mdates = new TreeSet<Integer>();
		int prevdt = 20141231;
		for (int i:dates)
			if (i%100==1)
				mdates.add(prevdt);
			else
				prevdt = i;
		return mdates;
	}

	public static SortedSet<Integer> dates()
	{
		return dates;
	}

    //No modification on static int key, but str_key can be updated via this API
	public static int setObjectKey(CBOType type, int objkey, int altKeyNum, String str_key)
	{
        Logger.log(Logger.SET_NODE_DATA, "setObjectKey("+type+","+objkey+","+altKeyNum + ","+str_key+")");
        //System.out.println();
        Set<Integer> attrs = FieldMeta.getAttrKeys(type, altKeyNum, new TreeSet<Integer>());

        String[] val = str_key.split("/");
        if (attrs.size() > 1 && attrs.size() != val.length)
            return -1; //invalid key

        NodeKeyDB db = nodes.get(type);
        if (objkey == -1)
        {
            objkey = db.read(str_key, altKeyNum);
            if (objkey < 0)
                objkey = db.nextkey();
        }
        db.upsert(objkey, altKeyNum, str_key);

        int idx = 0;
        for (int attr:attrs) {
            for (NodeDataDB dbd:nodevalues.get(type))
                dbd.upsert(objkey,attr,Utils.mindate(),val[idx]);
            idx++;
        }
        return objkey;
	}

	//return strkey1 of the object
	public static String getObjectKey(CBOType type, int key)
    {
        return nodes.get(type).read(key,1);
    }

	private static void filterObject(CBOType type, String strexpr, int asofdt, Map<Integer,Integer> out)
	{
		Map<String,Object>  eval = new HashMap<String,Object>();
		Map<Integer,Field> dbfld = new HashMap<Integer,Field>(); //Fld obj for singledb
		Map<String,Map<Integer,Field>> cols = new HashMap<String,Map<Integer,Field>>();

		//parse expr and vars to eval
		Expr expr = (strexpr == null || strexpr.trim().length() == 0)? null:Expr.getExpr(strexpr);
		if (expr == null) return;

		Set<String> s = expr.getVars(new HashSet<String>());

		//Load all values
		for (String f:s)
		{
			if (f.equals("strkey") || f.equals("objkey")) continue;
			FieldMeta m = FieldMeta.lookup(type, f);
			if (m == null) return;
			cols.put(f,new HashMap<Integer,Field>());
			for (NodeDataDB db: nodevalues.get(type))
				cols.get(f).putAll(db.readCol(m.index(), dbfld));
		}

		//Iterate all objkeys and remove anything that needs to be filtered
		for (Iterator<Map.Entry<Integer,Integer>> it = out.entrySet().iterator(); it.hasNext();)
		{
			Map.Entry<Integer,Integer> ent = it.next();
			eval.clear();
			eval.put("strkey", nodes.get(type).read(ent.getKey(), ent.getValue()));
			eval.put("objkey", ent.getKey());
			for (String f:s) {
				if (f.equals("strkey") || f.equals("objkey")) continue;
				Field fld = cols.get(f).get(ent.getKey());
				Object val = (fld == null)? null: fld.spoto();
				if (asofdt > 0 && fld != null) val = fld.geto(asofdt);
				if (val != null) eval.put(f, val);
			}
			if (!expr.filter(eval))
				it.remove();
		}

	}

	private static void sortObject(CBOType type, String sortflds, final int asofdt, Map<Integer,Integer> out)
	{
		Map<String,Object>  eval = new HashMap<String,Object>();
		Map<Integer,Field> dbfld = new HashMap<Integer,Field>(); //Fld obj for singledb
		final Map<String,Map<Integer,Field>> cols = new LinkedHashMap<String,Map<Integer,Field>>();

		//Load all values
		for (String f:sortflds.split(","))
		{
			cols.put(f,new HashMap<Integer,Field>());
			if (f.substring(1).equals("strkey") || f.substring(1).equals("objkey")) continue;
			FieldMeta m = FieldMeta.lookup(type, f.substring(1));
			if (m == null) return;
			for (NodeDataDB db: nodevalues.get(type))
				cols.get(f).putAll(db.readCol(m.index(), dbfld));
		}

		Comparator<Integer> c = new Comparator<Integer>() {
			public int compare(Integer k1, Integer k2) {
				for (String key:cols.keySet()) {
					int c = 0;
					if (key.substring(1).equals("objkey")) {
						c = k1.compareTo(k2);
					} else if (key.substring(1).equals("strkey")) {
						String sk1 = nodes.get(type).read(k1, out.get(k1));
						String sk2 = nodes.get(type).read(k2, out.get(k2));
						c = sk1.compareTo(sk2);
					} else {
						Map<Integer,Field> map = cols.get(key);
						if (!map.containsKey(k1)) return -1;
						if (!map.containsKey(k2)) return +1;
						Object k1v = (asofdt > 0)? map.get(k1).geto(asofdt):map.get(k1).spoto();
						Object k2v = (asofdt > 0)? map.get(k2).geto(asofdt):map.get(k2).spoto();
						c = map.get(k1).meta().fcompare(k1v,k2v);
					}
					if (c != 0) return (key.charAt(0) == '+')? c:-1*c;
				}
				return 0;
			}
		};

		List<Integer> outl = out.keySet().stream().sorted(c).collect(Collectors.toList());
		out.clear();
		for (Integer k:outl) out.put(k,0);
	}

    //Derives match->objkey for a given node (CBOType)
	//asofdt is strictly required to evaluate variables in strexpr
	//sortflds format +attr1,-attr2,-attr3...
	public static Map<Integer,Integer> getObjectKey(CBOType type,
			String strkey, String strexpr,
			int asofdt, String sortflds, Map<Integer,Integer> out)
    {
		//get all keys
		nodes.get(type).readAll(strkey,out);
		filterObject(type, strexpr,asofdt,out);
		if (sortflds != null && sortflds.length() > 0)
			sortObject(type, sortflds, asofdt, out);
        return out;
    }

    //Derives first extkey->objkey for a given node (CBOType)
	//public static int getObjectKey(CBOType type, String key)
    //{
    //    return nodes.get(type).read(key,1);
    //}

    //Derives linkkey->objkey for a given node (CBOType)
	public static int getObjectKey(LinkType ltype, CBOType ctype, int nodecnt, int link)
    {
        return edges.get(ltype).objkey(ctype,nodecnt,link);
    }

    //All matching links given partial obj keys
	public static List<Integer> getLinks(LinkType type, int[] objkeys,
		int asofdt, boolean includeObj, List<Integer> out)
    {
        return edges.get(type).read(objkeys, asofdt, includeObj, out);
    }

	public static int addLink(LinkType type, int[] objkeys, int fromdt, int todt)
    {
        return edges.get(type).upsert(objkeys, fromdt, todt);
	}

    public static void setFieldData(int obj, Field f)
    {
        Logger.log(Logger.SET_NODE_DATA, "setFieldData("+obj+","+f+")");

        CBOType type = CBOType.valueOf(f.meta().key());
        if (type != null) {
            for (NodeDataDB db:nodevalues.get(type))
                db.upsert(obj,f);
        } else {
            LinkType ltype = LinkType.valueOf(f.meta().key());
            for (EdgeDataDB db:edgevalues.get(ltype))
                db.upsert(obj,f);
        }
    }

    public static Map<Integer,Field> getLinkData(LinkType type, int obj, Map<Integer,Field> out)
    {
        for (EdgeDataDB db:edgevalues.get(type))
            db.readRow(obj,out);
        return out;
    }

    public static Map<Integer,Field> getObjectData(CBOType type, int obj, Map<Integer,Field> out)
    {
        for (NodeDataDB db:nodevalues.get(type))
            db.readRow(obj,out);
        return out;
    }
}
