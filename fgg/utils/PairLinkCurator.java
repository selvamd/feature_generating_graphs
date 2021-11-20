package fgg.utils;

import java.util.*;

//This class can be used for curation of pair relationships
//Curation enforce parent or child to be a mono-rel and instead of multi-rel
//Some Rels can further expect NoTimeGaps or expect time-staticness  
//The curation here does the typical case: NoTimeGaps=TRUE,static=FALSE
//       which ensures existance of exactly 1 rel at any point in time
public class PairLinkCurator
{
	public synchronized void makePkeyUnique() { 
        Set<Integer> set = new HashSet<Integer>();
        pkrel.forEach((k,v)-> set.add(Utils.msb(k)));
        set.forEach(key->makeunique(key,pkrel)); 
    }
	public synchronized void makeCkeyUnique() { 
        Set<Integer> set = new HashSet<Integer>();
        fkrel.forEach((k,v)->set.add(Utils.msb(k)));
        set.forEach(key->makeunique(key,fkrel)); 
    }
    public long[] times(int pk, int fk) { return pkrel.get(makeLong(pk,fk)); }

    public Set<Long> allkeys() { return pkrel.keySet(); }
    
	public int pkey(int key, int dt) { return monokey(key, dt, pkrel); }
	public int ckey(int key, int dt) { return monokey(key, dt, fkrel); }
    
	public Set<Integer> pkeys(int pk, int dt, Set<Integer> in) { return multikey(pk,dt,in,pkrel); }
	public Set<Integer> pkeys(int pk, Set<Integer> in) { return multikey(pk,in,pkrel); }
	public Set<Integer> ckeys(int pk, Set<Integer> in) { return multikey(pk,in,fkrel); }
	public Set<Integer> ckeys(int pk, int dt, Set<Integer> in) { return multikey(pk,dt,in,fkrel); }

	public void add(int pr, int cr, int from, int to) {
		long key = makeLong(pr,cr); 
		long val = makeLong(from,to);
		long[] vals = pkrel.get(key), newvals;
		if (vals == null) {
			newvals = new long[1];
			newvals[0] = val;
		} else {
			newvals = new long[vals.length+1];
			for (int i=0;i<vals.length;i++)
				newvals[i] = vals[i];
			newvals[vals.length] = val;
		}
		pkrel.put(makeLong(pr,cr),newvals);
		fkrel.put(makeLong(cr,pr),newvals);
	}

    ////////////////////////////////////////////////////////////////////////////////////
	private SortedMap<Long,long[]> pkrel = new TreeMap<Long,long[]>();
	private SortedMap<Long,long[]> fkrel = new TreeMap<Long,long[]>();
	
	private long makeLong(int msb, int lsb) {
		return (((long)(msb)) << 32) | (lsb & 0xffffffffL);
	}
    
    private Set<Integer> chkset = new HashSet<Integer>();
    private Map<Long,Integer> chkmap = new TreeMap<Long,Integer>();
	private void makeunique(int key, SortedMap<Long,long[]> relmap) 
    {
        chkmap.clear();
        for (int fk:multikey(key, chkset, relmap)) 
            for (long l:relmap.get(makeLong(key,fk)))
                chkmap.put(-1*l,fk);
        
        int expectedEnd = 99991231;
        for (long lkey:chkmap.keySet()) {
			int from = (int)((-1*lkey) >> 32);
            int fk = chkmap.get(lkey);
            long[] times = relmap.get(makeLong(key,fk));
            for (int i=0;i<times.length;i++)
                if (times[i] == (-1*lkey)) 
                    times[i] = makeLong(from,expectedEnd);
            expectedEnd = from; //Expected end of older period = begin of current period
        }
    }        

    
	private int monokey(int fk, int dt, SortedMap<Long,long[]> relmap) 
	{
		for (Map.Entry<Long,long[]> ent:relmap.tailMap(makeLong(fk,0)).entrySet())
		{
			long l = ent.getKey();
			int key = (int) l;
			if (fk != (l >> 32)) break;
			long[] vals = ent.getValue();
			for (int i=0;i<vals.length;i++)
			{
				int from = (int)(vals[i] >> 32);
				int to   = (int) vals[i];
				if (dt == from || (dt > from && dt < to))
					return key;
			}
		}
		return -1;
	}

	//use in case of many to many links 
	private Set<Integer> multikey(int pk, Set<Integer> in, SortedMap<Long,long[]> relmap) 
	{ 
		in.clear();
		for (Map.Entry<Long,long[]> ent:relmap.tailMap(makeLong(pk,0)).entrySet())
		{
			long l = ent.getKey();
			int key = (int) l;
			if (pk != (l >> 32)) break;
            in.add(key);
		}
		return in;
	}
    
	//use in case of many to many links 
	private Set<Integer> multikey(int pk, int dt, Set<Integer> in, SortedMap<Long,long[]> relmap) 
	{ 
		in.clear();
		for (Map.Entry<Long,long[]> ent:relmap.tailMap(makeLong(pk,0)).entrySet())
		{
			long l = ent.getKey();
			int key = (int) l;
			if (pk != (l >> 32)) break;
			long[] vals = ent.getValue();
			
			//Find the value on the date slot that applies
			for (int i=0;i<vals.length;i++)
			{
				int from = (int)(vals[i] >> 32);
				int to   = (int) vals[i];
				if (dt == from || (dt > from && dt < to))
				{
					in.add(key);
					continue;
				}
			}
		}
		return in;
	}
}
