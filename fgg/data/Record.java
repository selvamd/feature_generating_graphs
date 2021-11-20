package fgg.data;

import java.util.*;
import java.io.*;
import java.nio.*;

public class Record implements Serializable, Comparable<Record>
{
	private long recordid; //cbo id
	private int maxdate;
	private List<Field> values;

	public Record()  { 
		values = new ArrayList<Field>(); 
	}
	
	public Record(long id)  
	{ 
		recordid = id; 
		values   = new ArrayList<Field>(); 
	}
		
	public long recordid() 
	{
		return recordid;
	}

	public SortedSet<Integer> scddates(SortedSet<Integer> set) {
		set.clear();
		for (Field f:values)
			f.scddates(set);
		return set;
	}
	
	public int maxdate(int dt) 
	{ 
		return maxdate = Math.max(dt,maxdate);
	}

	public int mindate() 
	{ 
		int mindate = 99999999;
		for (Field f:values)
			mindate = Math.min(mindate,f.mindt());
		return mindate;
	}

    //Returns all obj keys linked together in this relationship
    public void getLinkObjs(LinkType type, int[] objs) 
    {
        for (int i=0;i<type.maxnodes();i++) {
            CBOType c = CBOType.valueOf(type.nodekey(i));
            objs[i] = (int)field(c.name().toLowerCase() +"_id").spotd();
        }
    }
    
    //Returns string key of the object
	public String getObjStrKey(List<FieldMeta> metas, int altkeynum)
	{
		String val = null;
		for (FieldMeta m:metas) {
            if (m.altKeyNum() != altkeynum) continue;
            Field f = field(m);
            if (f == null) return null;
            val = (val == null)? ""+f.geto(mindate()):
                    val+"/"+f.geto(mindate());
        }
		return val;	
	}
	
	public boolean storeKey(List<FieldMeta> metas, int altkeynum, String val)
	{
		String[] arr = val.split("/");
		int count = 0;
		for (FieldMeta m:metas) 
			if (m.altKeyNum() == altkeynum) 
				count++;
		if (count != arr.length) return false;
		for (FieldMeta m:metas) {
			if (m.altKeyNum() != altkeynum) continue;
            Field f = field(m);
            if (f == null) return false;
			f.seto(mindate(), val);
        }
		return true;	
	}
	
	public int compareTo(Record d)
	{
		if (recordid == d.recordid) return 0;
		return (recordid > d.recordid)? +1:-1;
	}

	public Field getOrAddField(FieldMeta m)
	{
		Field f = field(m);
		if (f != null) return f;
		f = new Field(m);
		values.add(f);
		return f;
	}
	
	public void addField(FieldMeta m)
	{
		Field f = field(m);
		if (f != null) return;
		values.add(new Field(m));
	}
	
	//Appends 2 records representing contigous timesequence
	public boolean append(Record next)
	{
		if (next.recordid != recordid) return false;
		if (next.values.size() != values.size()) return false;
		for (int i=0;i<values.size();i++)
			if (next.values.get(i).index() != values.get(i).index())
				return false;
		this.maxdate = Math.max(this.maxdate, next.maxdate);
		for (int i=0;i<values.size();i++)
			values.get(i).append(next.values.get(i));
		return true;
	}

	public Field field(FieldMeta m) 
	{
		for (Field f:values)
			if (f.index() == m.index())
				return f;
		return null;
	}
	
	public Field field(String fld) 
	{
		for (Field f:values)
			if (f.fname().equals(fld.toLowerCase()))
				return f;
		return null;
	}
	
	public int length()
	{
		int size = 16;
		for (Field f:values)
			size += f.length();
		return size;
	}

	public double getd(String field, int dt)
	{
		Field fld = field(field);
		return (fld == null)? 0:fld.getd(dt);
	}
	
	public Object geto(String field, int dt)
	{
		Field fld = field(field);
		return (fld == null)? null:fld.geto(dt);
	}

	public String gets(String field, int dt)
	{
		Field fld = field(field);
		return (fld == null)? null:fld.gets(dt);
	}
	
	//CHecks for completeness on a given date
	public boolean complete(int dt)
	{
		for (Field f:values)
			if (!f.complete(dt))
				return false;
		return true;
	}

	public String toString()
	{
		String res = ""+recordid+",";
		res += ""+maxdate+",{";
		for (Field f:values)
			res += f;
		res += "}";
		return res;
	}
	
	public void serialize(ByteBuffer buff) 
	{
		buff.putLong(recordid);
		buff.putInt(maxdate);
		buff.putInt(values.size());
		for (Field f:values)
			f.serialize(buff);
	}

	public boolean deserialize(ByteBuffer buff) { return deserialize(buff, null); }
	public boolean deserialize(ByteBuffer buff, List<FieldMeta> metas) {
		if (buff.remaining() < 12) 
			return false;
		
		recordid = buff.getLong();
		maxdate  = buff.getInt();
		
		//fields in a record
		int size = buff.getInt();
		
		for (int i=0;i<size;i++)
		{
			Field f = new Field();
			if (metas != null && !f.deserialize(buff, metas))
				return false;
			else if (metas == null && !f.deserialize(buff))
				return false;
			values.add(f);
		}
		return true;
	}
}
