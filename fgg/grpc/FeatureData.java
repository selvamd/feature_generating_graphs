package fgg.grpc;
import java.util.*;
import fgg.utils.*;
import fgg.data.*;

public class FeatureData
{
	//KEYSEQ can be sent multiple times for composite key
	//VALUEDT NOT followed by VALUE is interpreted as NULL
	//Sending ATTRKEY in the end is mandatory as msg terminator.
	public enum XMIT { KEYSEQ, VALUEDT, VALUE, ATTRKEY };

	//private GraphItem attr;
	private int attrkey;
	private SortedMap<Integer,Object> date2value;
	private int[] keyseq;
	private boolean singlekey;

	private FggDataServiceOuterClass.FggData last;
	private String error;

	public FeatureData()
	{
		date2value = new TreeMap<Integer,Object>();
		keyseq = new int[5];
		last  = null;
		error = null;
	}

	public FeatureData(GraphItem.EdgeAttr fld)
	{
		this();
		assert(fld != null);
		attrkey = fld.ordinal();
	}

	public FeatureData(GraphItem.NodeAttr fld)
	{
		this();
		assert(fld != null);
		attrkey = fld.ordinal();
		singlekey = true;
	}

	//////////////////// GETTERS AND SETTERS //////////////
	public GraphItem getField() { return GraphItem.findByPK(attrkey); }
	public String getError()    { return this.error; }
	public boolean isFullXmit() { return attrkey > 0 && error == null; }

	//if key needs to be looked up or formed
	public void addNodeKeyForLinks(GraphItem.Node type, int key) {
        addNodeKeyForLinks(type, 0, key);
    }
	public void addNodeKeyForLinks(GraphItem.Node type, int count, int key)
	{
		GraphItem.EdgeAttr attr = (GraphItem.EdgeAttr)getField();
		keyseq[attr.owner().index(type,count)] = key;singlekey = false;
	}

	//if key is available
	public void setLinkKey(int key) { reset(false);keyseq[0] = key;singlekey = true; }
	public void setObjKey(int key)  { reset(false);keyseq[0] = key;singlekey = true; }
	public void seto(int dt, Object val) { date2value.put(dt,val); }

	//Get Keyseq
	public CBOType  ctype() { return CBOType.valueOf(FieldMeta.lookup(attrkey).key());  }
	public LinkType ltype() { return LinkType.valueOf(FieldMeta.lookup(attrkey).key()); }

	public Object geto(int dt) {
        Object o = null;
        for (int date:date2value.keySet()) {
            if (date > dt) return o;
            o = date2value.get(date);
        }
        return o;
    }

    public Set<Integer> scddates() {
        return date2value.keySet();
    }

	//returns object or link key
	public int key()
	{
		LinkType type = ltype();
		if (type == null || keyseq[1]<=0)
			return keyseq[0];
		Set<Integer> res = type.index().findLink(keyseq, new HashSet<Integer>());
		if (res.size() == 1) return res.iterator().next();
		return 0;
	}

	public int getAttrKey() { return attrkey; }

	public void copy2rec(Record rec, FieldMeta m) { copy2field(rec.field(m)); }
	public void copy2field(Field f) {
		try {
			date2value.forEach((k,v) -> f.seto(k,v));
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(f.meta().fname());
			System.out.println(date2value);
		}
    }

	public void copy(int rec, Field f)
    {
        attrkey = f.index();
        setObjKey(rec);
        if (f != null)
            for (int dt=f.mindt();dt < 99999999; dt = f.nextScdDate(dt))
                date2value.put(dt, f.geto(dt));
        if (date2value.isEmpty())
            date2value.put(Utils.mindate(), f.meta().getDefault());
    }

	public void copy(Record rec, FieldMeta m)
	{
        reset(false);
        attrkey = m.index();
        setObjKey((int)rec.recordid());
        Field f = rec.field(m);
        if (f != null)
            for (int dt=f.mindt();dt < 99999999; dt = f.nextScdDate(dt))
                date2value.put(dt, f.geto(dt));
        if (date2value.isEmpty())
            date2value.put(rec.mindate(), m.getDefault());

        //copy((int)rec.recordid(),rec.field(m));
    }

	public String toString()
	{
		String res = "attr=" + attrkey;
		for (int key:keyseq)
		{
			res += "\nKey=" + key;
			if (singlekey) break;
		}
		res += "\nValue=" + date2value;
		return res;
	}

	//resets for reuse
	public void reset(boolean preserveKeys)
	{
		last  = null;
		error = null;
		date2value.clear();
		if (preserveKeys) return;
		for (int i=0;i<keyseq.length;i++)
			keyseq[i] = 0;
	}


	public List<FggDataServiceOuterClass.FggData> xmits()
	{
		List<FggDataServiceOuterClass.FggData> results = new ArrayList<FggDataServiceOuterClass.FggData>();

		for (int key:keyseq)
		{
			results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.KEYSEQ.ordinal()).setIntValue(key).build());
			if (singlekey) break;
		}

		Object prev = null;
		for (Map.Entry<Integer,Object> val:date2value.entrySet())
		{
			Object v = val.getValue();
			if (v == null && prev == null) continue;
			if (v != null)
			{
				if (v.equals(prev))	continue;
				results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.VALUEDT.ordinal()).setIntValue(val.getKey()).build());
				if (v instanceof Integer)
					results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.VALUE.ordinal()).setIntValue((Integer)v).build());
				else if (v instanceof Long)
					results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.VALUE.ordinal()).setLongValue((Long)v).build());
				else if (v instanceof Double)
					results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.VALUE.ordinal()).setDblValue((Double)v).build());
				else
					results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.VALUE.ordinal()).setStrValue(v.toString()).build());
			}
			else
				results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.VALUEDT.ordinal()).setIntValue(val.getKey()).build());
			prev = v;
		}

		results.add(FggDataServiceOuterClass.FggData.newBuilder().setField(XMIT.ATTRKEY.ordinal()).setIntValue(attrkey).build());
		return results;
	}

	//returns true to conclude object building
	public boolean add(FggDataServiceOuterClass.FggData data)
	{
		XMIT fld = XMIT.values()[data.getField()];
        if (fld == XMIT.KEYSEQ) {
            for (int i=0;i<keyseq.length;i++)
            {
                if (keyseq[i] > 0) continue;
                keyseq[i] = data.getIntValue();
                return false;
            }
        }

		if (last != null)
		{
			int lastval = last.getIntValue();
			last = null;
			if (fld == XMIT.KEYSEQ) {
				for (int i=0;i<keyseq.length;i++)
				{
					if (keyseq[i] > 0) continue;
					keyseq[i] = data.getIntValue();
					break;
				}
			} else if (fld == XMIT.VALUE) {
				switch (data.getValueCase())
				{
					case VALUE_NOT_SET: seto(lastval, null);break;
					case INT_VALUE:  	seto(lastval, new Integer(data.getIntValue())); break;
					case LONG_VALUE: 	seto(lastval, new Long(data.getLongValue()));break;
					case DBL_VALUE:  	seto(lastval, new Double(data.getDblValue()));break;
					case STR_VALUE:  	seto(lastval, data.getStrValue());break;
				}
			} else if (fld == XMIT.VALUEDT) {
				seto(lastval,null);
				last = data;
			} else if (fld == XMIT.ATTRKEY) {
				seto(lastval,null);
				attrkey = data.getIntValue();
				if (attrkey <= 0)
					error = "Invalid attr field";
				return true;
			}
		}
		else
		{
			if (fld == XMIT.VALUEDT) {
				last = data;
			} else if (fld == XMIT.KEYSEQ) {
				keyseq[0] = data.getIntValue();
			} else if (fld == XMIT.VALUE) {
				error = "VALUE without date";
			} else if (fld == XMIT.ATTRKEY) {
				attrkey = data.getIntValue();
				if (attrkey <= 0)
					error = "Invalid attr field";
				return true;
			}
		}
		return false;
	}

}
