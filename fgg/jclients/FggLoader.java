package fgg.jclients;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import fgg.utils.*;
import fgg.data.*;
import java.util.*;
import fgg.grpc.*;
import java.sql.*;
import java.util.concurrent.atomic.AtomicInteger;
import fgg.access.*;

public class FggLoader extends Persistor
{
	public int counter = 0;
	public int recordid = 0;

	private List<CBO> cbolist;
	private List<FeatureData> datalist;
	private List<CBOMeta> metalist;
	private FggClient client;
	private GraphItem.Node node;
	private String table;
	private String[] keys;
	private CBOMeta[] meta;

	public FggLoader(String table, String keylist) {
		this.table 		= table;
		this.keys       = keylist.split(",");
		this.client 	= new FggClient("localhost",33789);
	    this.cbolist 	= new ArrayList<CBO>();
	    this.datalist 	= new ArrayList<FeatureData>();
	    this.metalist 	= new ArrayList<CBOMeta>();
	}

	public void buildAttrList() throws Exception
	{
		this.node = (GraphItem.Node) GraphItem.findByName(table);
		meta  = CBOMeta.buildMeta("dev."+ table, "recordid,from_dt".split(","));
		for (int i=3;i<meta.length;i++)
		{
			GraphItem.NodeAttr attr = GraphItem.findAttr(node, meta[i].name.toLowerCase());
			if (attr == null) continue;
			datalist.add(new FeatureData(attr));
			metalist.add(meta[i]);
		}
	}


	public int persistSingleObject() throws Exception
	{
		if (cbolist.size() <= 0) return 0;
		String key = "";
		for (String akey:keys) key += "/" + cbolist.get(0).get(akey);
		int objkey = client.setObject(node, key.substring(1));
		for (int i=0;i<datalist.size();i++)
		{
			datalist.get(i).reset(false);
			datalist.get(i).setObjKey(objkey);
			for (CBO cbo:cbolist)
				datalist.get(i).seto(cbo.fromdt(), cbo.get(metalist.get(i)));
		}
		client.publishNonBlocking(datalist, new AtomicInteger(0));
		cbolist.clear();
		return 1;
    }

	public void extractObjectData() throws Exception
	{
        String filter = "";//" where recordid % 1000 = 0";
	    executeQuery(qdburl(), "select * from dev."+ table + filter, new QryProcessor() {
	        public boolean process(ResultSet rs) throws Exception
	        {
	            CBO cbo = new CBO(meta);
	            cbo.initQDB_Date(rs);
	            if (recordid != cbo.recid())
	            {
					counter += persistSingleObject();
					if (counter % 10000 == 0)
					System.out.println("Objects processed " + counter);
	            }
	            recordid = cbo.recid();
				cbolist.add(cbo);
	            return true;
	        }
	    });
		counter += persistSingleObject();
	}

	//OFFICER 0, HOUSEHOLD, CUSTOMER 2 , DEPOSIT, TRANCHE 4, LOAN, WEALTH 6,
	//TRANSACTIONS, LOANORIG 8 , INITIAL, INITCOMP 10, BRANCH, PRODUCT 12, REGION 13;
	public static void main( String[] args ) throws Exception
	{
		FggLoader loader = new FggLoader("Customer", "cust_key");
		loader.client.connect();
		loader.buildAttrList();
		long l1 = System.currentTimeMillis();
		loader.extractObjectData();
		System.out.println("Total objects processed " + loader.counter +
			" in " + ((System.currentTimeMillis()-l1)/1000) + " secs");
	}

}
