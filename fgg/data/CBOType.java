package fgg.data;

import java.util.*;
import fgg.utils.*;
import fgg.access.*;

public class CBOType
{
	private int leg;
	private int root;
	private int parent;
	private String name;
	private final int nodekey;
	public Recorder recorder;

	//Object keys are defined at root node level
	private int objectkey;
	public int nextObjectKey() {
		CBOType type = valueOf(root);
		return ++type.objectkey;
	}

	private static Map<Integer,CBOType> key2cbo = new HashMap<Integer,CBOType>();

	private CBOType(int recid) {
		this.nodekey = recid;
		key2cbo.put(recid,this);
	}

	public String name()  	{  return name; 	}
	public boolean isRoot() { return nodekey == root; }
	public int leg() 		{ return leg; 	 	}
	public int root() 		{ return root; 	 	}
	public int parent() 	{ return parent; 	}
	public int ordinal() 	{  return nodekey;  }

	public static CBOType valueOf(int key) {
		return key2cbo.get(key);
	}

	public static CBOType valueOf(String name)
	{
		for (CBOType type:key2cbo.values())
			if (type.name.equalsIgnoreCase(name))
				return type;
		return null;
	}

	public String toString() {
		return this.name;
	}

	public static Collection<CBOType> values() {
		return key2cbo.values();
	}

	public static void init(Map<String,CBO> cbos, Map<String,String> maxkeys)
	{
		key2cbo.clear();
		for (CBO cbo:cbos.values())
		{
			CBOType type = new CBOType(cbo.recid());
			type.name = cbo.get("node_name");
			type.parent = Integer.parseInt(cbo.get("parent_node_key"));
			type.root = Integer.parseInt(cbo.get("root_node_key"));
			type.leg = Integer.parseInt(cbo.get("leg_node_key"));
			String str = maxkeys.get(""+type.nodekey);
			type.objectkey = (str == null)? 100:Integer.parseInt(str);
		}
	}

}
