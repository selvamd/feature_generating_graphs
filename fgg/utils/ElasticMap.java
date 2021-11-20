package fgg.utils;

import java.util.*;

//This class is extension of recorder to cache 
//strkey to recordid lookups. r
public class ElasticMap
{
    //hashkey -> seq+idx []
	private final Map<Integer,long[]> map;
    private int maxkeyseq = 0;
	
	public ElasticMap()
	{
		map = new HashMap<Integer,long[]>();
	}

	public Map<Integer,int[]> remapByRecId() 
    {
        final Map<Integer,int[]> res = new HashMap<Integer,int[]>();
        map.forEach((k,v)-> {
            for (long l:v) 
            {
                int[] out = res.get(Utils.lsb(l));
                if (out == null) out = new int[maxkeyseq];
                //alt key seq starts from 1.  
                out[Utils.msb(l)-1] = k;
                res.put(Utils.lsb(l), out);
            }
        });
        return res;
    }

	public void map(String key, int idx, int seq)
	{
        map(key.hashCode(), idx,seq);
	}
    
	public void map(int khash, int idx, int seq) 
    {
        maxkeyseq = Math.max(seq,maxkeyseq);
		long[] vals = map.get(khash);
		long l = Utils.makelong(seq,idx);
		if (vals == null) {
			map.put(khash, new long[] { l });
		} else {
			for (int i=0;i<vals.length;i++)
				if (l == vals[i])
					return;
			long[] newvals = new long[vals.length+1];
			for (int i=0;i<vals.length;i++)
				newvals[i] = vals[i];
			newvals[vals.length] = l;
			map.put(khash, newvals);
		}
    }
    
	
	public Map<Integer,Integer> find(String key, Map<Integer,Integer> result) 
	{
		result.clear();
		if (key != null && key.trim().length() > 0)
		{
			if (!map.containsKey(key.hashCode())) 
				return result;
			for (long l:map.get(key.hashCode())) 
				result.put(Utils.lsb(l),Utils.msb(l));
		} else {
			for (long[] ids:map.values())
				for (long l:ids)
					if (Utils.msb(l) == 1)
						result.put(Utils.lsb(l),Utils.msb(l));
		}
		return result;
	}

}
