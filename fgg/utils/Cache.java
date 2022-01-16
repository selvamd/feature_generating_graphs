package fgg.utils;

import fgg.access.*;
import fgg.access.Persistor;
import fgg.data.*;
import java.util.*;

import java.util.*;

public class Cache
{
	private static SortedSet<Integer> dates;

	public static void init() {
		try {
            EnumGroup.load();
            CBOBuilder.buildAll();
			buildDates();
			buildRecorders();
			Persistor.addShutdownHook();
			System.out.println("Cache Initialized");
		} catch (Exception e) {
			System.out.println("Cache Initialization failed");
			e.printStackTrace();
		}
	}

	private static void buildRecorders()
	{
		Set<Integer> result = new HashSet<Integer>();
		for (CBOType type:CBOType.values())
		{
			if (!type.isRoot()) continue;
			FieldMeta.getAttrKeys(type,result);
			//if (result.size() == 0) continue;
			List<FieldMeta> list = new ArrayList<FieldMeta>();
			for (int attrkey:result) list.add(FieldMeta.lookup(attrkey));
			type.recorder = FileIndexRecorder.make(type.name(), list, new ElasticMap(), null);
			//type.recorder = FileRecorder.make(type.name(), list, new ElasticMap(), null);
            //type.recorder = MongoRecorder.make(type, list);
		}

		for (LinkType type:LinkType.values())
		{
			FieldMeta.getAttrKeys(type,result);
			//if (result.size() == 0) continue;
			List<FieldMeta> list = new ArrayList<FieldMeta>();
			for (int attrkey:result) list.add(FieldMeta.lookup(attrkey));
			type.recorder = FileIndexRecorder.make(type.name(), list, null, type.index());
			//type.index().buildIndex();
			//type.recorder = FileRecorder.make(type.name(), list, null, type.index());
            //type.recorder = MongoRecorder.make(type, list);
		}
	}

	private static void buildDates() throws Exception
	{
		//////////////////////// Initialized valid row dates //////////////////
		dates = new TreeSet<Integer>();
		int dt = Utils.mindate(), today = Integer.parseInt(Persistor.today().replaceAll("-", ""));
		while (dt < today) {
			 dates.add(dt);
			 dt = Persistor.addDate(dt,1);
		}
		System.out.println("Loaded valid history dates");
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

	public static int setObjectKey(CBOType type, int altkeynum, int objkey, String str_key)
	{
        if (objkey > 0)
            type.recorder.storeKey(objkey, altkeynum, str_key);
		return objkey;
	}

    /*
	//Create new if objkey is 0
	public static int setObjectKey(CBOType type, int altkeynum, int objkey, String str_key)
	{
        //getObjectKey(CBOType type, String match, Map<Integer,Integer> result)
		if (objkey > 0 && !type.recorder.has(objkey)) return 0;
		if (objkey == 0) {
            //Skip duplicates
            Map<Integer,Integer> map = type.recorder.findKey(str_key, new HashMap<Integer,Integer>());
            for (Map.Entry<Integer,Integer> e:map.entrySet())
                if (e.getValue().intValue() == altkeynum)
                    return e.getKey();
            objkey = type.nextObjectKey();
        }
		type.recorder.storeKey(objkey, altkeynum, str_key);
		return objkey;
	}
     */

	public static Map<Integer,Integer> getObjectKey(CBOType type, String match, Map<Integer,Integer> result)
	{
		return type.recorder.findKey(match, result);
	}

	public static Record getRecord(int typekey, int key)
	{
		CBOType type = CBOType.valueOf(typekey);
		if (type != null) return type.recorder.get(key);
		LinkType ltype = LinkType.valueOf(typekey);
		return (ltype == null)? null:ltype.recorder.get(key);
	}

	public static void setRecord(int typekey, Record rec)
	{
        //System.out.println(rec);
		CBOType type = CBOType.valueOf(typekey);
		if (type != null) type.recorder.add(rec);
		LinkType ltype = LinkType.valueOf(typekey);
		if (ltype != null) ltype.recorder.add(rec);
	}

	public static int addLink(LinkType type, int[] objkeys, int fromdt, int todt)
  	{
		if (objkeys.length != type.maxnodes()) return -1;
		int linkkey = type.index().createLink(objkeys, fromdt, todt);
		if (type.recorder.has(linkkey)) return linkkey;
		Record rec = type.recorder.newrecord(linkkey);
		//store linkkey to recorder
		rec.field("isvalid").setd(fromdt,1);
		rec.field("isvalid").setd(todt,0);
		for (int i=0;i<type.maxnodes();i++)
		{
			CBOType c = CBOType.valueOf(type.nodekey(i));
			rec.field(c.name().toLowerCase() +"_id").setd(objkeys[i]);
		}
        type.recorder.add(rec);
		return linkkey;
	}
}
