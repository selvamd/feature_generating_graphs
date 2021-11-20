package fgg.data;

import java.util.*;
import java.io.*;
import java.nio.*;
import fgg.utils.*;
import java.text.*;
import fgg.utils.*;
import fgg.access.*;

//This is a type system
public class FieldMeta
{
    private int fieldid;
    private String fname;
    private DataType dtype;
    private FieldType ftype;
    private int nodekey;
    private int edgekey;
    private int fsize;
    private int esize;
    private int altkeynum;

    private boolean deploy;
    private boolean obfuscate;
    private boolean curate;
    private boolean fill_by_key;
    private boolean fill_by_date;
    private boolean is_enum;
    private boolean split_by_leg;

    //core fields only
    private String dbname;
    private String tblname;
    private String colname;

    //Virtual fields
    private PathExpr pathexpr;

    //For static and dynamic fields
    private String src_file;

    //Applies for all derived fields.
    //If ftype=VIRTUAL, input_mkeys.length == 1
    private int[] input_mkeys;

    public String toString() { return "fieldId=" + fieldid + ",fname=" + fname + "," + key(); }

    ////////////////////  VARIOUS ACCESSOR METHODS /////////////////////////////////////
    public static final Map<Integer,FieldMeta> fields = new HashMap<Integer,FieldMeta>();
    public static FieldMeta lookup(int index)    { return fields.get(index); }

	public static FieldMeta lookup(CBOType type, String name)
	{
		for (int key:getAttrKeys(type, new HashSet<Integer>()))
			if (lookup(key).fname().equals(name))
				return lookup(key);
		return null;
	}

	public static FieldMeta lookup(LinkType type, String name)
	{
		for (int key:getAttrKeys(type, new HashSet<Integer>()))
			if (lookup(key).fname().equals(name))
				return lookup(key);
		return null;
	}

	public static SortedSet<Integer> getAttrKeys(CBOType type, int altKeyNum, SortedSet<Integer> result)
	{
		getAttrKeys(type, result);
		result.removeIf(attrkey -> lookup(attrkey).altKeyNum() != altKeyNum);
		return result;
	}

	public static Set<Integer> getAttrKeys(CBOType type, Set<Integer> result)
	{
		result.clear();
		while (true)
		{
			for (FieldMeta m:fields.values())
				if (m.isNode() && m.key() == type.ordinal())
					result.add(m.index());
			if (type.isRoot()) break;
			type = CBOType.valueOf(type.parent());
			if (type == null) break;
		}
		return result;
	}

	public static Set<Integer> getAttrKeys(LinkType type, Set<Integer> result)
	{
		result.clear();
		for (FieldMeta m:FieldMeta.fields.values())
			if (m.key() == type.ordinal())
				if (!m.isNode())
					result.add(m.index());
		return result;
	}

	public static Set<Integer> getAttrKeys(boolean isnodes, Set<Integer> result)
	{
		result.clear();
		for (FieldMeta m:FieldMeta.fields.values())
			if (m.isNode() == isnodes)
				result.add(m.index());
		return result;
	}
    //////////////////// END ACCESSORS ////////////////////////////

    private FieldMeta() {}

    public static FieldMeta fromCBO(CBO cbo)
    {
        FieldMeta m = new FieldMeta();
        try {
            m.fieldid   = cbo.recid();
            m.fname     = cbo.get("attr_name");
            m.dtype     = DataType.valueOf(cbo.get("dtype"));
            m.ftype     = FieldType.valueOf(cbo.get("refresh_type"));
            m.nodekey   = (cbo.get("node_key")==null)? 0:Integer.parseInt(cbo.get("node_key"));
            m.edgekey   = (cbo.get("edge_key")==null)? 0:Integer.parseInt(cbo.get("edge_key"));

			m.is_enum   	= (Integer.parseInt(cbo.get("is_enum")) == 1);
            m.fill_by_key   = (Integer.parseInt(cbo.get("fill_all_key")) == 1);
            m.fill_by_date  = (Integer.parseInt(cbo.get("fill_all_time")) == 1);
            m.deploy        = (Integer.parseInt(cbo.get("is_deploy")) == 1);
            m.obfuscate     = (Integer.parseInt(cbo.get("is_pii")) == 1);
            m.curate        = (Integer.parseInt(cbo.get("is_curational")) == 1);
			m.split_by_leg  = (Integer.parseInt(cbo.get("split_by_leg")) == 1);

            if (m.is_enum) 	
            {
                m.esize = Integer.parseInt(cbo.get("maxsize"));
                m.dtype = DataType.ENUM;
                m.fsize = 4;
            } else 
                m.fsize = Integer.parseInt(cbo.get("maxsize"));
            m.altkeynum		= Integer.parseInt(cbo.get("alt_key_number"));

            if (m.ftype == FieldType.CORE)
            {
                m.dbname = m.tblname = m.colname = "";
                //m.dbname  = cbo.get("dbname");
                //m.tblname = cbo.get("tblname");
                //m.colname = cbo.get("colname");
                if (m.dbname == null || m.tblname == null || m.colname == null)
                    return null;
            } else {
                m.src_file = cbo.get("src_file");
                if (m.ftype == FieldType.VIRTUAL)
                    m.pathexpr = new PathExpr(cbo.get("filter_expr"));
            }
        } catch (Exception e) {
			e.printStackTrace();
            return null;
        }
        fields.put(m.index(), m);
        return m;
    }

    public int index()      		{ return this.fieldid; }
    public String fname()   		{ return this.fname;   }
    public DataType dtype() 		{ return this.dtype;   }
    public boolean isNode()			{ return this.nodekey > 0;  }
    public int key()        	    { return Math.max(nodekey,edgekey); }
    public int altKeyNum()    		{ return altkeynum; 	 }
    public int size()               { return this.fsize;     }
    public int esize()             	{ return this.esize;     }
    public boolean deploy()     	{ return this.deploy;    }
    public boolean obfuscate()      { return this.obfuscate; }
    public boolean isfcd()          { return false;          }
    public boolean splitByLeg()     { return this.split_by_leg; }
    public boolean isderived()      { return this.pathexpr != null; }
    public PathExpr expr()          { return this.pathexpr; }

    public String getDefault()
    {
        switch(dtype)
        {
            case DATE:
                return "19000100";
            case BYTE:
            case CHAR:
            case SHORT:
            case INT:
            case LONG:
            case KEY:
            case FLOAT:
            case DOUBLE:
                return "0";
            case ENUM:
                return null;
        }
        return null;
    }

    public boolean fequals(Object o1, Object o2)
    {
        //if (fname.endsWith("share_affiliate_cd"))
        //        System.out.println(fname+"["+o1 + "=" + o2+"]");
        if (o1 == null && o2 == null) return true;
        if (o1 == null || o2 == null) {
            String comp = (dtype == DataType.STRING)? "":"0";
            return (o1 == null)? o2.toString().equals(comp):o1.toString().equals(comp);
        }
        //if (ftype == FieldType.LINK)
        //    return o1.equals(o2);

        if (dtype == DataType.STRING)
            return o1.toString().trim().equals(o2.toString().trim());

        try {
            return o1.equals(o2) || o2d(o1)==o2d(o2);
        } catch (Exception e) {
            return false;
        }
    }

    public boolean isNumber()
    {
        //LINK fields are treated as single
        //compound field for set and get
        //if (ftype == FieldType.LINK) return false;
        if (dtype == DataType.STRING) return false;
        //everything else has numeric representation
        return true;
    }

    public double o2d(Object o)
    {
		if (o == null) return (dtype == DataType.KEY)? -1:0;
        String s = o.toString();
        switch(dtype)
        {
            case BYTE:
            case CHAR:
                return s.charAt(0);
            case DATE:
                s = (s.split(" "))[0];
                s = s.replaceAll("-","");
            case SHORT:
            case INT:
            case LONG:
            case KEY:
                return Long.parseLong(s);
            case FLOAT:
            case DOUBLE:
                return Double.parseDouble(s);
            case ENUM:
                return EnumGroup.add(fname,s); //Creates a new index if required
        }
        return 0;
    }

    public Serializable toArray(int size)
    {
        switch(dtype)
        {
            case DATE:
            case KEY:
            case ENUM:
            case INT:     return new int[size];
            case BYTE:      return new byte[size];
            case CHAR:     return new char[size];
            case SHORT:     return new short[size];
            case LONG:     return new long[size];
            case FLOAT:     return new float[size];
            case DOUBLE: return new double[size];
            case STRING: return new String[size];
        }
        return null;
    }

    public Object getArrayo(Serializable s, int idx)
    {
        if (s == null) return null;
        double d = getArrayd(s,idx);

        switch(dtype)
        {
            case CHAR:     return (""+(char)d);
            case DATE:
            case KEY:
            case BYTE:
            case SHORT:
            case INT:     return new Integer((int)d);
            case LONG:     return new Long((long)d);
            case FLOAT:
            case DOUBLE: return new Double(d);
            case ENUM:     return EnumGroup.value((int)d);
            case STRING: return ((String[])s)[idx];
        }
        return null;
    }

    public double getArrayd(Serializable s, int idx)
    {
        if (s == null) return 0;
        switch(dtype)
        {
            case DATE:
            case KEY:
            case ENUM:
            case INT:     return ((int[])s)[idx];
            case BYTE:      return ((byte[])s)[idx];
            case CHAR:     return ((char[])s)[idx];
            case SHORT:     return ((short[])s)[idx];
            case LONG:     return ((long[])s)[idx];
            case FLOAT:     return ((float[])s)[idx];
            case DOUBLE: return ((double[])s)[idx];
        }
        return 0;
    }

    public void setArray(Serializable s, int idx, Object o)
    {

        if (dtype == DataType.STRING)
            ((String[])s)[idx] = (o == null)? "":o.toString().trim();
        else
            setArray(s,idx,o2d(o));
    }

    public void setArray(Serializable s, int idx, double d) {

        switch(dtype)
        {
            case DATE:
            case KEY:
            case ENUM:
            case INT:     ((int[])s)[idx]    = (int)d;return;
            case BYTE:      ((byte[])s)[idx]   = (byte)d;return;
            case CHAR:     ((char[])s)[idx]   = (char)d;return;
            case SHORT:     ((short[])s)[idx]  = (short)d;return;
            case LONG:     ((long[])s)[idx]   = (long)d;return;
            case FLOAT:     ((float[])s)[idx]  = (float)d;return;
            case DOUBLE: ((double[])s)[idx] = (double)d;return;
        }
    }

    //Deserializes from a bytebuffer
    public Serializable toArray(ByteBuffer buff, int size)
    {
        if (buff.remaining() < size * fsize)
            return null;

        Serializable s = toArray(size);

        byte[] b = new byte[fsize];
        for (int i=0;i<size; i++)
        {
            switch(dtype)
            {
                case DATE:
                case KEY:
                case ENUM:
                case INT:     ((int[])s)[i]    = buff.getInt();break;
                case BYTE:      ((byte[])s)[i]   = buff.get();break;
                case CHAR:     ((char[])s)[i]   = buff.getChar();break;
                case SHORT:     ((short[])s)[i]  = buff.getShort();break;
                case LONG:     ((long[])s)[i]   = buff.getLong();break;
                case FLOAT:     ((float[])s)[i]  = buff.getFloat();break;
                case DOUBLE: ((double[])s)[i] = buff.getDouble();break;
                case STRING:
                    buff.get(b);
                    int len = 0;
                    for (;len < fsize;len++)
                        if (b[len] == 0)
                            break;
                    ((String[])s)[i] = new String(b,0,len);
                    break;
            }
        }
        return s;
    }

    public String dbStr(Object o)
    {
        String result = null;
        if (o == null) return null;
        if (isNumber())
        {
            if (dtype == DataType.DOUBLE || dtype == DataType.FLOAT)
            {
                double d = o2d(o);
                DecimalFormat df = new DecimalFormat("#.###");
                result = df.format(d);
            }
            else result = o.toString();
        }
        else result = o.toString();
        return (result.equals(getDefault()))? null:result;
    }

    //Serializes into a bytebuffer
    public void fromArray(ByteBuffer buff, Serializable s, int size)
    {
        for (int i=0;i<size; i++)
        {
            switch(dtype)
            {
                case DATE:
                case KEY:
                case ENUM:
                case INT:     buff.putInt(((int[])s)[i]);break;
                case BYTE:      buff.put(((byte[])s)[i]);break;
                case CHAR:     buff.putChar(((char[])s)[i]);break;
                case SHORT:     buff.putShort(((short[])s)[i]);break;
                case LONG:     buff.putLong(((long[])s)[i]);break;
                case FLOAT:     buff.putFloat(((float[])s)[i]);break;
                case DOUBLE: buff.putDouble(((double[])s)[i]);break;
                case STRING:
                    String str = ((String[])s)[i];
                    for (int j=0;j<fsize; j++)
                        if (j < str.length())
                            buff.put((byte)str.charAt(j));
                        else buff.put((byte)0);
            }
        }
    }

}
