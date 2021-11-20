package fgg.utils;

import java.util.*;

public class Index
{
	private final long[] refs;
	private int refsize;
	private boolean indexed;

	public Index(int size)
	{
		refsize = 0; indexed = true;
		this.refs = new long[size];
	}

	public void buildIndex()
	{
		Arrays.sort(refs,0,refsize);
		indexed = true;
	}

	public void compress()
	{
			int j = 0;
			for (int i=0;i<refsize;i++)
				if (Integer.MAX_VALUE != (int)refs[i])
					refs[j++] = refs[i];
			refsize = j;
	}

	public int pk(int idx) { return (int)(refs[idx]>>32); }
	public int fk(int idx) { return (int)(refs[idx]); }

	//adds without any duplicate checks
	//Assumes buildIndex is called externally
	public void add(long key, int val)
	{
		long l = (((long)((int)key)) << 32) | (val & 0xffffffffL);
		//System.out.println("Added=>"+key + ":" + val + ":" + l);
		refs[refsize++] = l;
		indexed = false;
	}

	public void add(String key, int val)
	{
		long l = (((long)key.hashCode()) << 32) | (val & 0xffffffffL);
		//System.out.println("Added str=>"+key + ":" + val + ":" + l);
		refs[refsize++] = l;
		indexed = false;
	}

	//return -1 if hashkey not found
	public int get(long key) { return getp((int)key); }
	public int get(String key) { return getp(key.hashCode()); }
	public void del(String key, int val) { delp(key.hashCode(), val); }
	public void del(long key, int val) { delp((int)key, val); }
	public int size() { return this.refsize; }

	public int xnext(int idx)
	{
		if (idx+1 == refsize)
			return -1;
		int x0 = (int)(refs[idx] >> 32);
		int	y0 = (int) refs[idx];
		idx++;
		int x1 = (int)(refs[idx] >> 32);
		int	y1 = (int) refs[idx];
		return (x0 != x1 || y1 == Integer.MAX_VALUE)? -1:idx;
	}

	private int getp(int hash)
	{
		if (!indexed) throw new RuntimeException("Cannot get before index is built");
		int x = 0, y = 0;
		long l = (((long)hash) << 32) | (y & 0xffffffffL);
		int idx = Arrays.binarySearch(refs,0,refsize,l);
		if (idx < 0) idx = -1 * (idx+1);
		for (; idx < refsize; idx++)
		{
			x = (int)(refs[idx] >> 32);
			y = (int) refs[idx];
			if (x < hash) continue;
			if (y == Integer.MAX_VALUE)
				return -1;
			if (x > hash) break;
			return idx;
		}
		return -1;
	}

	private void delp(int hash, int val)
	{
		int idx = getp(hash);
		if (idx < 0) return;
		for (int i = idx;i<refsize;i++)
		{
			if (hash != (int)(refs[i] >> 32)) break;
			//Swap until delete entry is pushed to the end.
			//Delete op has already happened if swap is required
			if (i > 0 && refs[i] < refs[i-1])
			{
				long l = refs[i-1];
				refs[i-1] = refs[i];
				refs[i] = l;
			}
			else if (val == (int)refs[i]) // perform delete op
			{
				refs[i] = (((long)hash) << 32) | (Integer.MAX_VALUE & 0xffffffffL);
			}
		}
	}

}
