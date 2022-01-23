package fgg.access;

import java.io.*;
import java.net.*;
import java.util.*;
import java.sql.*;
import java.math.*;
import fgg.data.*;
import fgg.utils.*;

public class CBOBuilder extends CBOPersistor
{
		public static enum Table { NODE, NODE_TREE, OBJECT, OBJECT_LEG, EDGE, LINKS, ATTR, GIT_HISTORY, ATTR_FILL_STATE };
		private static CBOMeta[][] meta_list;

		private static int leg_key;  //Key generator for legs
		private static int type_key; //Key generator for node,edge,attr
		private static int linkrec;
		private static int legrec;
		private static int treerec;
        private static int fillrec;

        public static void buildMeta() throws Exception {
			if (meta_list == null)
			{
				meta_list = new CBOMeta[Table.values().length][];
				for (Table tbl:Table.values())
					meta_list[tbl.ordinal()] = CBOMeta.buildMeta("dbo_"+tbl.toString().toLowerCase(), "recordid,from_dt".split(","));
			}
        }

        public static void buildCBOTypes() throws Exception {
            //Obj keys from each node is its own namespace
			String sql = "select root_node_key, max(object_key) as maxobjkey from dbo_object group by root_node_key";
			CBOType.init(loadCBO(Table.NODE),lookup(qdburl(),sql, 1));
        }

        public static void buildLinkTypes() throws Exception {
            //Link keys from each edge is its own namespace
			String sql = "select edge_key, max(link_key) as maxlinkkey from dbo_links group by edge_key";
			LinkType.init(loadCBO(Table.EDGE),lookup(qdburl(),sql, 1));
        }

        public static void buildAttrs() throws Exception {
            Map<String,CBO> cbos = loadCBO(CBOBuilder.Table.ATTR);
            for (CBO cbo:cbos.values()) FieldMeta.fromCBO(cbo);
            System.out.println("Total attrs loaded " + FieldMeta.fields.size());
        }

        public static void buildMaxKeys() throws Exception {
			leg_key = getMaxVal(100, Table.OBJECT_LEG, "leg_key");

            //Single typekey for nodes, edges and attrs.
            //Init value is set to maxof(nodekey,edgekey,attrkey)
			type_key = getMaxVal(100, Table.NODE, "node_key");
			type_key = getMaxVal(type_key, Table.EDGE, "edge_key");
			type_key = getMaxVal(type_key, Table.ATTR, "attr_key");

            // These are just recordids and not system keys. Simply set to next safe value
			linkrec = getMaxVal(100, Table.LINKS, "recordid");
			treerec = getMaxVal(100, Table.NODE_TREE, "recordid");
			legrec  = getMaxVal(100, Table.OBJECT_LEG, "recordid");
			fillrec  = getMaxVal(100, Table.ATTR_FILL_STATE, "recordid");
        }

        public static void buildLinkIndex() throws Exception
        {
            for (LinkType type:LinkType.values())
            {
                LinkIndex index = new LinkIndex(type);
                try {
                    Map<String,CBO> cbos = CBOPersistor.loadAllCBO(
                        CBOMeta.buildMeta("dbo_links", "link_key,from_dt".split(",")), "edge_key = " + type.ordinal(), null);
                    for (CBO cbo:cbos.values()) {
                        int[] key = new int[type.maxnodes()];
                        for (int i=0;i<key.length;i++)
                            key[i] = Integer.parseInt(cbo.get("object_key"+i));
                        index.loadLink(Integer.parseInt(cbo.get("link_key")), key,
                                    Integer.parseInt(cbo.get("from_dt")),
                                    Integer.parseInt(cbo.get("to_dt")));
                    }
                } catch (Exception e) {
                    System.out.println("Initialization error. No Links are created for " + type.name());
                    e.printStackTrace();
                    return;
                }
                type.setIndex(index);
            }
        }

		//Perform initialzation for CBOBuilding
		public static void buildAll() throws Exception
		{
            buildMeta();
            buildCBOTypes();
            buildLinkTypes();
            buildAttrs();
            buildLinkIndex();
            buildMaxKeys();
			System.out.println("CBOBuilder Initialized");
		}


		private static int getMaxVal(int id, Table tbl, String field) throws Exception {
			String val = getSingleVal(qdburl(), "select max(" +field+ ") from dbo_" + tbl.toString().toLowerCase());
			return (val == null)? id:Math.max(id,Integer.parseInt(val));
		}

		public static int getAttrLastUpdateDt(int attr) throws Exception {
			String val = getSingleVal(qdburl(), "select max(from_dt) from dbo_attr_fill_state where attr_key = " + attr);
			return (val == null)? Utils.mindate():Integer.parseInt(val);
		}

		public static Map<String,CBO> loadCBO(Table tbl) throws Exception {
			return loadAllCBO(meta_list[tbl.ordinal()], null, null);
		}

		public static void persist(Table tbl, Collection<CBO> c) throws Exception {
			persistnew(meta_list[tbl.ordinal()], c);
		}

		public static CBO createNode(String name, int parent, int root)
		{
			int id = ++type_key;
			CBOMeta[] meta = meta_list[Table.NODE.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], id);
			cbo.set(meta[4], name);
			cbo.set(meta[5], parent);
			cbo.set(meta[6], root);
			cbo.set(meta[7], 0);
			return cbo;
		}

		public static CBO createRootNode(String name, int leg)
		{
			int id = ++type_key;
			CBOMeta[] meta = meta_list[Table.NODE.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], id);
			cbo.set(meta[4], name);
			cbo.set(meta[5], id);
			cbo.set(meta[6], id);
			cbo.set(meta[7], leg);
			return cbo;
		}

		public static CBO createNodeTree(int parent, int child)
		{
			CBOMeta[] meta = meta_list[Table.NODE_TREE.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], ++treerec);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], parent);
			cbo.set(meta[4], child);
			return cbo;
		}

		//node_key,alt_key_number creates a namespace within which strkeys can uniquely map to objkey
		public static CBO createObject(int nodekey, int[] attrkeys, String[] attrvals)
		{
			String csv = null;
			CBOType rnode = CBOType.valueOf(nodekey);
			int id = rnode.nextObjectKey();
			CBOMeta[] meta = meta_list[Table.OBJECT.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], id);
			cbo.set(meta[4], nodekey);
			cbo.set(meta[5], rnode.root());

			for (int key:attrkeys)
				csv = (csv == null)? ""+key:csv+","+key;
			cbo.set(meta[5], csv);

			csv = null;
			for (String str:attrvals)
			{
				if (str == null) str = "";
				csv = (csv == null)? str.trim():csv+","+str.trim();
			}
			cbo.set(meta[6], csv);
			return cbo;
		}

		public static CBO createObjectLeg(int legkey, int aggr_nodekey, int aggr_objkey, int leg_objkey)
		{
			CBOMeta[] meta = meta_list[Table.OBJECT_LEG.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], ++legrec);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], legkey);
			cbo.set(meta[4], aggr_nodekey);
			cbo.set(meta[5], aggr_objkey);
			cbo.set(meta[6], leg_objkey);
			return cbo;
		}

		public static CBO createEdge(String name, boolean isDefault, int[] nodekeys, String[] names, int[] limits)
		{
			int id = ++type_key;
			CBOMeta[] meta = meta_list[Table.EDGE.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], id);
			cbo.set(meta[4], name);
            cbo.set(meta[5], (isDefault)? 1:0);
			int size = nodekeys.length;
			cbo.set(meta[6], size);
			for (int i=0;i<size;i++)
				cbo.set("node_key"+i, ""+nodekeys[i]);
			for (int i=0;i<size;i++)
				cbo.set("node_name"+i, ""+names[i]);
			for (int i=0;i<size;i++)
				cbo.set("node_limits"+i, ""+ limits[i]);
			return cbo;
		}

		public static CBO createLink(int edgekey, int[] objkey, int fromdt, int todt)
		{
			LinkType type = LinkType.valueOf(edgekey);
			int linkkey = type.index().createLink(objkey, fromdt, todt);
			CBOMeta[] meta = meta_list[Table.LINKS.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], ++linkrec);
			cbo.set(meta[1], fromdt);
			cbo.set(meta[2], todt);
			cbo.set(meta[3], linkkey);
			cbo.set(meta[4], edgekey);
			int size = type.maxnodes();
			for (int i=0;i<size;i++)
				cbo.set(meta[5+i], objkey[i]);
			return cbo;
		}

		public static CBO createAttr(CBOType node, String name, DataType dtype, FieldType ftype, int size, Map<String,String> properties)
		{
			int id = ++type_key;
			CBOMeta[] meta = meta_list[Table.ATTR.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], id);
			cbo.set(meta[4], name.toLowerCase());
			cbo.set(meta[5], dtype.toString());
			cbo.set(meta[6], ftype.toString());
			cbo.set(meta[7], size);
			for (String str:properties.keySet())
					cbo.set(str,properties.get(str));
			cbo.set("node_key", ""+node.ordinal());
			return cbo;
		}


		public static CBO createAttr(LinkType edge, String name, DataType dtype, FieldType ftype, int size, Map<String,String> properties)
		{
			int id = ++type_key;
			CBOMeta[] meta = meta_list[Table.ATTR.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], -1);
			cbo.set(meta[2], -1);
			cbo.set(meta[3], id);
			cbo.set(meta[4], name);
			cbo.set(meta[5], dtype.toString());
			cbo.set(meta[6], ftype.toString());
			cbo.set(meta[7], size);
			for (String str:properties.keySet())
					cbo.set(str,properties.get(str));
			cbo.set("edge_key", ""+edge.ordinal());
			return cbo;
		}

		public static int makeAltKey(int nodekey, int seqNum, SortedSet<Integer> flds) {
			try {
				String csv = "";
				for (int m:flds) csv += ","+m;
				executeUpdate(qdburl(), "update dbo_attr set alt_key_number = " + seqNum + " where attr_key in (" + csv.substring(1) + ")");
			}
			catch (Exception e) {
				return 0;
			}
			return seqNum;
		}

		public static CBO createGitHistory() 	{ return null; }

		public static CBO createAttrFillState(int attr, int dt)
        {
			int id = ++fillrec;
			CBOMeta[] meta = meta_list[Table.ATTR_FILL_STATE.ordinal()];
			CBO cbo = new CBO(meta);
			cbo.set(meta[0], id);
			cbo.set(meta[1], dt);
			cbo.set(meta[2], dt);
			cbo.set(meta[3], attr);
			cbo.set(meta[4], 0);
			return cbo;
        }

		public static Map<String,String> getAttrProperties(boolean isenum)
		{
			Map<String,String> prop = new HashMap<String,String>();

			String fields = "inputs,src_file,link_path,filter_expr,reducer";
			for (String str:fields.split(","))
				prop.put(str,"");

			fields = "fill_all_key,fill_all_time,is_curational,is_pii,split_by_leg,alt_key_number";
			for (String str:fields.split(","))
				prop.put(str,"0");

			prop.put("is_deploy", "1");
			prop.put("is_enum",(isenum)? "1":"0");

			return prop;
		}
}
