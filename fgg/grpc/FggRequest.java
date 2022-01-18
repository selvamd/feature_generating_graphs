package fgg.grpc;

import io.grpc.stub.StreamObserver;
import io.grpc.stub.CallStreamObserver;
import java.util.*;
import fgg.data.*;
import fgg.utils.*;
import fgg.access.*;

//Will encapsulate all request
//request based state
public class FggRequest
{
	private static int REQUESTID = 100;
	public final int request;
	public final CBOType cbo;
	public final String   expr;
	public final int exprdt;
	public final LinkType link;
	private final int[] keys;
	private final Map<Integer,Field> attrs;
	private final Map<Integer,Field> res;
	private final Map<Integer,EdgeQry> queries;

	//Will read all link based features for a batch of objkeys from parent
	public static class EdgeQry {
		public final int request;
		public final LinkType link;
		public final int asofdt;
		public final int nodepos;
		public final FggRequest parent;
		private final Map<Integer,Field> attrs;
		private final Map<Integer,Field> res;

		private EdgeQry(FggRequest p, LinkType type, int node, int asof) {
			parent = p;link = type;nodepos = node;asofdt = asof;
			request = FggRequest.REQUESTID++;
			attrs = new LinkedHashMap<Integer,Field>();
			res = new HashMap<Integer,Field>();
		}

		public void addAttr(int attr) {
			FieldMeta m = FieldMeta.lookup(attr);
			Field f = new Field(m);
			f.seto(Utils.mindate(),m.getDefault());
			attrs.put(attr,f);
		}

		public List<FeatureData> feature(int pageno, int pgsize, int idx, List<FeatureData> data)
		{
			data.clear();

			if (pageno * pgsize + idx >= parent.keys.length)
				return data;

			/*
			int objkey = keys[pageno * pgsize + idx];
			objkey = Cache2.getObjectKey(link,parent.cbo,nodepos,objkey);
			res = Cache2.getObjectData(ctype, instkey, res);

			for (int attr:attrs.keySet()) {
				data.add(new FeatureData());
				data.get(data.size()-1).copy(instkey, res.containsKey(attr)?
					res.get(attr):attrs.get(attr));
			}
			*/
			return data;
		}
	}

	public FggRequest(CBOType ctype, LinkType ltype, String exp, int dt, Collection<Integer> out) {
		request = REQUESTID++;
		cbo = ctype;
		link = ltype;
		expr = exp;
		exprdt = dt;
		keys = out.stream().mapToInt(Integer::intValue).toArray();
		//attrs = new LinkedHashSet<Integer>();
		attrs = new LinkedHashMap<Integer,Field>();
		res = new HashMap<Integer,Field>();
		queries = new HashMap<Integer,EdgeQry>();
	}

	public EdgeQry createEdgeQry(LinkType type,int nodecnt, int asofdt) {
		EdgeQry qry = new EdgeQry(this,type,nodecnt,asofdt);
		queries.put(qry.request,qry);
		return qry;
	}

	public void addAttr(int attr) {
		FieldMeta m = FieldMeta.lookup(attr);
		Field f = new Field(m);
		f.seto(Utils.mindate(),m.getDefault());
		attrs.put(attr,f);
	}

	public boolean bLastPage(int pageno, int pgsize) {
		return (pageno * pgsize + pgsize >= keys.length);
	}

	public List<FeatureData> feature(int pageno, int pgsize, int idx, List<FeatureData> data)
	{
		data.clear();

		if (pageno * pgsize + idx >= keys.length)
			return data;

		int instkey = keys[pageno * pgsize + idx];

		if (cbo != null) {
			Cache2.getObjectData(cbo, instkey, res);
		} else if (link != null) {
			Cache2.getLinkData(link, instkey, res);
		} else return data;

		for (int attr:attrs.keySet()) {
			data.add(new FeatureData());
			data.get(data.size()-1).copy(instkey, res.containsKey(attr)?
				res.get(attr):attrs.get(attr));
		}
		return data;
	}
}
