package fgg.data;

import java.util.*;
import fgg.utils.*;
import fgg.access.*;

//Expects cbo_link to be populated for all relationships
public class LinkType
{
	private int[] nodekey;
	private int[] nodelimits;
	private String name;
	private final int edgekey;
	private int nodesize;
	private LinkIndex index;
	private boolean isDefault = false;

	public Recorder recorder;

	//Link keys are unique per link type
	private int linkkey;
	public int nextLinkKey() { return ++linkkey; }

	private static Map<Integer,LinkType> key2cbo = new HashMap<Integer,LinkType>();

	public String toString() 
    {
		return this.name;
	}

	private LinkType(int recid) 
    {
		this.edgekey = recid;
		key2cbo.put(recid,this);
	}

	public int ordinal() {
		return edgekey;
	}

	public int maxnodes() { return nodesize; }

	//Creates consistency checks on the constraints
	//returns true if all objects of this nodekey must
	//have at least one of this relationship link
	public boolean isMandatory(int key) {
		for (int i=0;i<maxnodes();i++)
			if (nodekey[i] == key)
				return (nodelimits[i]%10 > 0);
		return false;
	}

	//Creates consistency checks on the constraints
	//returns true if an object of this nodekey can have
	//more than one relationship link at the same time
	public boolean isMultiple(int key) {
		for (int i=0;i<maxnodes();i++)
			if (nodekey[i] == key)
				return (nodelimits[i]/10 > 1);
		return true;
	}

	public int nodekey(int idx) {
		return (idx < 0 || idx > 4)? 0:nodekey[idx];
	}

	public String name()  {
		return name;
	}

	public LinkIndex index()  {
		return index;
	}

	public void setIndex(LinkIndex idx) {
		index = idx;
	}

    public boolean has(CBOType ctype) {
        return cboindex(ctype) >= 0; 
    }

	public int cboindex(CBOType ctype) {
        return cboindex(ctype, 0);
    }
    
	public int cboindex(CBOType ctype, int count)
	{
        int cnt = 0;
		for (int i=0;i<maxnodes();i++)
			if (nodekey[i] == ctype.ordinal())
                if (cnt++ == count)
                    return i;
		return -1;
	}

	public static LinkType valueOf(int key) {
		return key2cbo.get(key);
	}

	//Returns default link between 2 cbos
	public static LinkType valueOf(CBOType parent, CBOType child)
  	{
		if (parent == null || child == null) return null;
		for (LinkType ltype:key2cbo.values()) 
        {
			if (ltype.maxnodes() != 2) continue;
            if (parent == child && ltype.cboindex(parent,1) >= 0 && ltype.isDefault)
                    return ltype;
			if (parent != child && ltype.has(parent) && 
                ltype.has(child) && ltype.isDefault)
                    return ltype;
		}
		return null;
	}

	public static LinkType valueOf(CBOType[] types)
	{
        Map<CBOType,Integer> map = new HashMap<CBOType,Integer>();
        
        for (CBOType type:types) 
        {
            if (map.containsKey(type))
                map.put(type,1+map.get(type));
            else map.put(type,1);
        }
        
		for (LinkType ltype:key2cbo.values()) 
        {
            int count = 0;
            for (CBOType ctype:types) {
                int val = map.get(ctype);
                if (ltype.cboindex(ctype,val-1) >= 0)
                    count += val;
            }
            if (types.length == count)
                return ltype;
		}
		return null;
	}

	public static LinkType valueOf(String name)
	{
		for (LinkType type:key2cbo.values())
			if (type.name.equalsIgnoreCase(name))
				return type;
		return null;
	}

	public static Collection<LinkType> values()
	{
		return key2cbo.values();
	}

	public static void init(Map<String,CBO> cbos, Map<String,String> maxkeys)
	{
		key2cbo.clear();
		for (CBO cbo:cbos.values())
		{
			LinkType type = new LinkType(cbo.recid());
			type.name = cbo.get("edge_name");
			type.nodesize = Integer.parseInt(cbo.get("node_size"));
			type.nodekey = new int[type.nodesize];
			type.nodelimits = new int[type.nodesize];
			type.isDefault = "1".equals(cbo.get("default_edge"));
			String str = maxkeys.get(""+type.edgekey);
			type.linkkey = (str == null)? 100:Integer.parseInt(str);
			for (int i=0;i<type.nodesize;i++)
			{
				//20 is most permissive (0 min and n max) and set as default
				type.nodekey[i] = Integer.parseInt(cbo.get("node_key"+i));
				str = cbo.get("node_limits"+i);
				type.nodelimits[i] = (str != null)? 20:Integer.parseInt(str);
			}
		}
	}

}
