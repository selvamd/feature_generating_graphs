package fgg.data;

import java.util.*;
import java.util.stream.*;
import java.io.*;
import java.nio.*;
import fgg.utils.*;

public class Field implements Serializable
{
	private int findex; // field index 
	private FieldMeta meta; // field index 
	private List<Scd> values;
	private static final int INIT_DT = Utils.mindate();

	public Field()  { 
		values = new ArrayList<Scd>(); 
	}
	
	public int index() { return findex; }
	
	public FieldMeta meta() { return (meta != null)? meta:FieldMeta.lookup(findex); }
	public String fname() { return meta().fname(); }

	public Field(FieldMeta m)  { 
		meta = m;
		findex = m.index(); 
		values = new ArrayList<Scd>(); 
	}
	
	public Field(int id)  { 
		findex = id; 
		values = new ArrayList<Scd>(); 
	}
 
	public int mindt() { return nextScdDate(0); }

	public SortedSet<Integer> scddates(SortedSet<Integer> set) {
		for (Scd s:values)
			set.add(s.fromdt());
		return set;
	}

	public int nextScdDate(int dt) 
	{
		for (Scd s:values)
            if (s.fromdt() > dt)
                return s.fromdt();
		return 99999999;
	}
	
	/*
	public void link(int val)
	{
		FieldMeta meta = meta();
		if (meta.ftype() != FieldType.LINK) return; 
		Set<Integer> set = (values.size() == 0)? new HashSet<Integer>():
				Arrays.stream((int[])geto(INIT_DT)).boxed().collect(Collectors.toSet());
		set.add(val);
		seto(INIT_DT, (int[]) set.stream().mapToInt(i -> i).toArray());
	}

	public boolean haslink(int val)
	{
		FieldMeta meta = meta();
		if (meta.ftype() != FieldType.LINK) return false; 
		if (values.size() == 0) return false;
		int[] vals = (int[])geto(INIT_DT);
		if (vals == null) return false;
		for (int v:vals) 
			if (v == val)
				return true;
		return false;
	}
	
	public void delink(int val)
	{
		FieldMeta meta = meta();
		if (meta.ftype() != FieldType.LINK) return; 
		if (values.size() == 0) return;
		Set<Integer> set = Arrays.stream((int[])geto(INIT_DT)).boxed().collect(Collectors.toSet());
		set.remove(val);
		if (set.size() > 0)
			seto(INIT_DT, (int[]) set.stream().mapToInt(i -> i).toArray());
		else 
			values.clear();
	}*/

	//Appends 2 fields representing contigous timesequence
	public void append(Field next)
	{
		FieldMeta meta = meta();
		//if (meta.ftype() == FieldType.LINK) 
		//	return; //Link doesnot change for time series

		int from, to;
		for (int i=0;i < next.values.size();i++)
		{
			from = to = next.values.get(i).fromdt();
			
			if (meta.isfcd()) 
				to += 30;
			
			for (int dt = from;dt <= to; dt++)
			{
				if (meta.isNumber())
					setd(next.getd(dt));
				else
					seto(next.geto(dt));
			}
		}
	}
	
	public boolean complete(int dt)
	{
		FieldMeta meta = meta();
		Object o = geto(dt);
		//if (meta.ftype() == FieldType.LINK) 
		//	return (o != null && ((int[])o).length > 0);
		if (values.size() == 0) return false;
		Scd s = values.get(0);
		return (s.fromdt() <= dt); 
	}

	public boolean seto(Object o) { return seto(INIT_DT, o); }

    /*
	public boolean seto(int dt, Object o)
	{
		FieldMeta meta = meta();
		if (meta.fequals(geto(dt),o)) 
			return false;
		
		Scd prev = null;
		int pos = 0;
		for (Scd s:values)
		{
			if (!s.has(dt, meta)) 
				break;
			prev = s;
			pos++;
		}
		
		if (pos == 0 || !prev.has(dt,meta) || 
			((!meta.isfcd()) && prev.fromdt() != dt))
		{
			Scd s = new Scd(dt, meta);
			s.seto(dt, meta, o);
			values.add(pos, s);
		} else prev.seto(dt, meta, o);
		return true;
	}*/

	public boolean seto(int dt, Object o)
	{
		FieldMeta meta = meta();
        int index = -1;
        for (Scd s:values) 
            if (s.fromdt() <= dt) 
                index++;
        if (index >= 0 && meta.fequals(values.get(index).geto(dt,meta),o)) 
            return false; 
        else if (index >= 0 && values.get(index).fromdt() == dt)
            values.get(index).seto(dt, meta, o); 
        else if (index + 1 < values.size() && meta.fequals(values.get(1+index).geto(dt,meta),o))
            values.get(1+index).reset_dt(dt); 
        else {
			Scd s = new Scd(dt, meta);
			s.seto(dt, meta, o);
			values.add(index+1, s);
        }
        return true;
    }
	
	public boolean setd(double d) { return setd(INIT_DT, d); }

	public boolean setd(int dt, double d)
	{
		FieldMeta meta = meta();
		if (!meta.isNumber()) return false;
        int index = -1;
        for (Scd s:values) 
            if (s.fromdt() <= dt) 
                index++;
        
        if (index >= 0 && values.get(index).getd(dt,meta) == d) 
            return false;
        else if (index >= 0 && values.get(index).fromdt() == dt)
            values.get(index).setd(dt, meta, d);
        else if (index + 1 < values.size() && values.get(1+index).getd(dt,meta) == d)
            values.get(1+index).reset_dt(dt);
        else {
			Scd s = new Scd(dt, meta);
			s.setd(dt, meta, d);
			values.add(index+1, s);
        }
        return true;
    }
    
    /*
	public boolean setd(int dt, double d)
	{
		FieldMeta meta = meta();
		if (!meta.isNumber()) return false;
		if (getd(dt) == d) return false;
		
		Scd prev = null;
		int pos = 0;
		for (Scd s:values)
		{
			if (!s.has(dt, meta)) 
				break;
			prev = s;
			pos++;
		}
		
		if (pos == 0 || !prev.has(dt,meta) || 
			((!meta.isfcd()) && prev.fromdt() != dt))
		{
			Scd s = new Scd(dt, meta);
			s.setd(dt, meta, d);
			values.add(pos, s); 
		} else prev.setd(dt, meta, d);
		return true;
	} */

	public String spots() {  return meta().dbStr(spoto()); }
	public String gets(int dtidx) { return meta().dbStr(geto(dtidx)); }

	public Object spoto() {
		Scd s = (values.size() == 0)? null:values.get(values.size()-1);
		if (s == null) return null;
		return s.spoto(meta());
	}
	
	public double spotd() {
		Scd s = (values.size() == 0)? null:values.get(values.size()-1);
		if (s == null) return 0;
		return s.spotd(meta());
	}
	
	public Object geto(int dtidx)
	{
		FieldMeta meta = meta();
		Scd prev = null;
		for (Scd s:values)
		{
			if (!s.has(dtidx, meta)) 
				break;
			prev = s;
		}
		return (prev == null)? null:prev.geto(dtidx, meta);				
	}
	
	public double getd(int dtidx)
	{
		FieldMeta meta = meta();
		if (!meta.isNumber())
			return 0;
		
		Scd prev = null;
		for (Scd s:values)
		{
			if (!s.has(dtidx, meta))
				break;
			prev = s;
		}
		return (prev == null)? 0:prev.getd(dtidx, meta);				
	}
	
	public int length()
	{
		FieldMeta meta = meta();
		int size = 8;
		for (Scd s:values)
			size += s.length(meta);
		//System.out.println(size);
		return size;
	}

	public String toString()
	{
		FieldMeta meta = meta();
		String res = meta.fname() + "\n";
		for (Scd s:values)
			res += "\t" + s + s.geto(s.fromdt(),meta) + "\n";
		return res;
	}
	
	public void serialize(ByteBuffer buff) 
	{
		FieldMeta meta = meta();
		buff.putInt(meta.index());
		buff.putInt(values.size());
		for (Scd s:values)
			s.serialize(buff,meta);
	}

	public boolean deserialize(ByteBuffer buff, List<FieldMeta> metas) 
	{
		if (buff.remaining() < 8) 
			return false;
		
		findex = buff.getInt();
		for (FieldMeta me: metas)
			if (me.index() == findex)
				this.meta = me;

			if (meta == null) 
			return false;
		
		int size = buff.getInt();
		for (int i=0;i<size;i++)
		{
			Scd s = new Scd();
			if (!s.deserialize(buff, meta)) 
				return false;
			values.add(s);
		}
		
		return true;
	}
	
	public boolean deserialize(ByteBuffer buff) 
	{
		if (buff.remaining() < 8) 
			return false;
		
		findex = buff.getInt();
		FieldMeta meta = meta();
		
		int size = buff.getInt();
		for (int i=0;i<size;i++)
		{
			Scd s = new Scd();
			if (!s.deserialize(buff, meta)) 
				return false;
			values.add(s);
		}
		
		return true;
	}
	
}
