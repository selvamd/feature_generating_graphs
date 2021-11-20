package fgg.utils;

import java.util.*;
import java.io.*;
import fgg.data.*;
import fgg.access.*;

//LinkIndex is also backed by more space efficient primitive array based cache.
//Once cache is built using create/load, then call buildIndex to move the data
//to primitive caches afterwhich this class becomes read-only.
//The *_z functions use the primitives

//edge,link,node,obj,times[]

public class LinkIndex
{
	private final LinkType type;
	private List<SortedSet<Long>> obj2link;
	private List<SortedSet<Long>> link2obj;

	private List<Index> zobj2link;
	private List<Index> zlink2obj;
	private Index ztimes;
	private int[]  zendtimes;

	//Map of linkkey to valid timeperiod
	private Map<Integer,long[]> times;

    //Link data is also loaded from database
    //Link data is loaded and stored temprorarily in file
    private DataOutputStream outstream;

	public LinkType type() { return type; }

	public LinkIndex(LinkType typ)
	{
		this.type 	  = typ;
		this.times 	  = new TreeMap<Integer,long[]>();
		this.obj2link = new ArrayList<SortedSet<Long>>();
		this.link2obj = new ArrayList<SortedSet<Long>>();

		this.zobj2link = new ArrayList<Index>();
		this.zlink2obj = new ArrayList<Index>();
		
		for (int i=0;i<typ.maxnodes();i++) 
		{
			this.obj2link.add(new TreeSet<Long>());
			this.link2obj.add(new TreeSet<Long>());
		}
        //loadLinksFromFile();
	}

	public int createLink(int[] objs, int fromdt, int todt)
	{
		if (objs.length != obj2link.size()) return 0;
		for (int key:objs) if (key <= 0) return 0;

		Set<Integer> out = findLink(objs, new HashSet<Integer>());
        if (out.isEmpty()) {
            int newkey = loadLink(type.nextLinkKey(), objs, fromdt, todt);
            //storeLinkToFile(type.nextLinkKey(), objs, fromdt, todt);
            return newkey;
        }
        else return (out.size() == 1)? out.iterator().next():-1;
	}

	public int loadLink(int link, int[] objs, int fromdt, int todt)
	{
		/////// VALIDATE ///////////////
		if (link == 0) return 0;
		if (objs.length != obj2link.size()) return 0;
		for (int key:objs) if (key <= 0) return 0;

		/////// INSERT OBJKEYS ///////////////
		for (int i = 0;i < objs.length;i++) {
			obj2link.get(i).add(Utils.makelong(objs[i],link));
			link2obj.get(i).add(Utils.makelong(link,objs[i]));
		}

		long[] vals = times.get(link), newvals;
		if (vals == null) {
			newvals = new long[1];
			newvals[0] = Utils.makelong(fromdt,todt);
		} else {
			newvals = new long[vals.length+1];
			for (int i=0;i<vals.length;i++)
				newvals[i] = vals[i];
			newvals[vals.length] = Utils.makelong(fromdt,todt);
		}
		times.put(link,newvals);
		return link;
	}

    
	public void buildIndex() 
    {
		zobj2link.clear();
		zlink2obj.clear();
		for (int i=0; i < type.maxnodes(); i++) {
			final int idx = i;
			//Build obj2link
			zobj2link.add(new Index(obj2link.get(i).size()));
			obj2link.get(i).forEach((l)-> zobj2link.get(idx).add(Utils.msb(l),Utils.lsb(l)));
			zobj2link.get(i).buildIndex();
			//Build link2obj
			zlink2obj.add(new Index(link2obj.get(i).size()));
			link2obj.get(i).forEach((l)-> zlink2obj.get(idx).add(Utils.msb(l),Utils.lsb(l)));
			zlink2obj.get(i).buildIndex();
		}
		int sum = times.values().stream().mapToInt(x->x.length).sum();
		ztimes = new Index(sum);
		zendtimes = new int[sum];
		for (Integer link:times.keySet()) {
			for (long l: times.get(link)) {
				zendtimes[ztimes.size()] = Utils.lsb(l);
				ztimes.add(link, Utils.msb(l));
			}
		}
		obj2link.clear();
		link2obj.clear();
		times.clear();
		times = null;
	}

    
	private int objkey_z(int linkkey, CBOType next) 
    {
		for (int i=0; i < type.maxnodes(); i++) {
			if (type.nodekey(i) != next.ordinal()) continue;
			int idx = zlink2obj.get(i).get(Utils.makelong(linkkey,0));
			return (idx < 0)? -1:zlink2obj.get(i).fk(idx);
		}
		return -1;
	}

	private int getLink_z(int[] objs)
	{
		Set<Integer> out = findLink(objs, new HashSet<Integer>());
		return (out.size() != 1)?  -1:out.iterator().next();
	}

	private int filterByTime_z(int linkkey, int dt)
	{
		for (int i = ztimes.get(Utils.makelong(linkkey,0)); i >= 0; i = ztimes.xnext(i))
			if (dt >= ztimes.fk(i) && dt <= zendtimes[i])
				return linkkey;
		return -1;
	}

	private SortedMap<Integer,Integer> getTimes_z(int link, SortedMap<Integer,Integer> map)
	{
		map.clear();
		for (int i = ztimes.get(Utils.makelong(link,0)); i >= 0; i = ztimes.xnext(i))
			map.put(ztimes.fk(i),zendtimes[i]);
		return map;
	}

	//search by partial key(s). Key(s) to ignore are set to -1
	private Set<Integer> findLink_z(int[] obj, Set<Integer> out)
	{
		out.clear();
		int size = Math.min(obj.length, obj2link.size());
		int maxcount = 0;
		Map<Integer,Integer> matches = new HashMap<Integer,Integer>();
		for (int i=0;i<size;i++)
		{
			if (obj[i]==0) continue;
			maxcount++;
			int idx = zobj2link.get(i).get(Utils.makelong(obj[i],0));
			if (idx < 0) continue;
			int link = zobj2link.get(i).fk(idx);
			matches.put(link, matches.containsKey(link)? 1+matches.get(link):1);
		}

        for (Map.Entry<Integer,Integer> ent:matches.entrySet())
            if (maxcount == ent.getValue())
                out.add(ent.getKey());

		return out;
	}
    
	public int objkey(int linkkey, CBOType next) 
    {
		if (times == null) return objkey_z(linkkey,next);
		for (int i=0; i < type.maxnodes(); i++) {
			if (type.nodekey(i) != next.ordinal()) continue;
			for (long l:link2obj.get(i).tailSet(Utils.makelong(linkkey,0)))
			{
				if (Utils.msb(l) != linkkey) break;
				return Utils.lsb(l);
			}
		}
		return -1;
	}

	public int getLink(int[] objs)
	{
		if (times == null) return getLink_z(objs);
		Set<Integer> out = findLink(objs, new HashSet<Integer>());
		return (out.size() != 1)?  -1:out.iterator().next();
	}

    public int filterByTime(int linkkey, int dt)
	{
		if (times == null) return filterByTime_z(linkkey,dt);
		long[] time = times.get(linkkey);
		if (time == null) return -1;
		for (long t:time)
			if (dt >= Utils.msb(t) && dt <= Utils.lsb(t))
				return linkkey;
		return -1;
	}

	public SortedMap<Integer,Integer> getTimes(int link, SortedMap<Integer,Integer> map)
	{
		if (times == null) return getTimes_z(link,map);
		map.clear();
		long[] time = times.get(link);
		if (time == null) return map;
		for (long t:time) map.put(Utils.msb(t),Utils.lsb(t));
		return map;
	}

	public Set<Integer> findLink(int[] obj, Set<Integer> out)
	{
		if (times == null) return findLink_z(obj,out);
		out.clear();
		int size = Math.min(obj.length, obj2link.size());
		int maxcount = 0;
		Map<Integer,Integer> matches = new HashMap<Integer,Integer>();
		for (int i=0;i<size;i++)
		{
			if (obj[i]==0) continue;
			maxcount++;
			for (long l:obj2link.get(i).tailSet(Utils.makelong(obj[i],0)))
			{
				if (Utils.msb(l) != obj[i]) break;
				Integer cnt = matches.get(Utils.lsb(l));
				if (cnt == null) matches.put(Utils.lsb(l),1);
				else matches.put(Utils.lsb(l),1+cnt);
			}
		}

        for (Map.Entry<Integer,Integer> ent:matches.entrySet())
            if (maxcount == ent.getValue())
                out.add(ent.getKey());
            
		return out;
	}
/*
//called when new links are created to append to file
private synchronized void storeLinkToFile(int link, int[] objs, int fromdt, int todt)
{
	try {
		if (outstream == null)
			outstream = new DataOutputStream(new FileOutputStream(
					Persistor.filestorepath() +"link"+type.ordinal()+".ser", true));
		outstream.writeInt(link);
		for (int i=0;i<objs.length;i++)
				outstream.writeInt(objs[i]);
		outstream.writeInt(fromdt);
		outstream.writeInt(todt);
		outstream.flush();
	} catch (Exception e) {
	}
}

//Loads valid links from file system during construction
private void loadLinksFromFile() {
	try {
		DataInputStream stream = new DataInputStream(new FileInputStream(
				Persistor.filestorepath() + "link"+type.ordinal()+".ser"));
		int link, fromdt, todt;
		int[] objs = new int[this.obj2link.size()];
		while(stream.available() > 0) {
			link = stream.readInt();
			for (int i=0;i<objs.length;i++)
				objs[i] = stream.readInt();
			fromdt = stream.readInt();
			todt = stream.readInt();
			loadLink(link, objs, fromdt, todt);
		}
		stream.close();
	} catch (Exception e) {
	}
}
*/
}
