package fgg.grpc;

import io.grpc.stub.StreamObserver;
import io.grpc.stub.CallStreamObserver;
import java.util.*;
import fgg.data.*;
import fgg.utils.*;
import fgg.access.*;

public class FggService extends FggDataServiceGrpc.FggDataServiceImplBase
{
	@Override
    public void queryData(FggDataServiceOuterClass.FggMsg request,
        StreamObserver<FggDataServiceOuterClass.FggMsg> responseObserver)
	{
		MsgType type = MsgType.values()[request.getRequest()];
		switch (type)
		{
			case LOGIN:  				request = onLogin(request);break;
			case GET_DATES:				request = onGetDates(request);break;

			case GET_NODES:				request = onGetNodes(request);break;
			case GET_EDGES:				request = onGetEdges(request);break;
			case GET_ATTRS:				request = onGetAttrs(request);break;

			case GET_NODE_INFO:			request = onGetNodeInfo(request);break;
			case GET_EDGE_INFO:			request = onGetEdgeInfo(request);break;
			case GET_ATTR_INFO:			request = onGetAttrInfo(request);break;

			case GET_OBJ_KEYS:			request = onGetObjKeys(request);break;
			case GET_LINK_KEYS:			request = onGetLinkKeys(request);break;
			case SET_OBJ_KEY:			request = onSetObjKey(request);break;
			case SET_LINK_KEY:			request = onSetLinkKey(request);break;

			case TASK_REQUEST:			request = onTaskRequest(request);break;
			case ADD_ATTR:				request = onAddAttr(request);break;
			case NOTIFY_GIT_CHECKIN:	request = onNotifyGitCheckin(request);break;
			case NOTIFY_CBO_REFRESH:	request = onNotifyCBORefresh(request);break;
		}
		responseObserver.onNext(request);
		responseObserver.onCompleted();
    }

	private FggDataServiceOuterClass.FggMsg onLogin(FggDataServiceOuterClass.FggMsg request) {
		String user = getParam(request,"user");
		String pwd  = getParam(request,"pass");
		System.out.println(user + ":" + pwd);
		return create(request).addValues(addparam("STATUS","OK")).build();
	}

	private FggDataServiceOuterClass.FggMsg onGetDates(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		for (int dt:Cache.dates())
			bldr.addOutkey(dt);
		bldr.addValues(addparam("STATUS","OK"));
		return bldr.build();
	}

	//returns node ids of all nodes
	private FggDataServiceOuterClass.FggMsg onGetNodes(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);

		for (CBOType type:CBOType.values())
			bldr.addOutkey(type.ordinal());

		bldr.addValues(addparam("STATUS","OK"));
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onGetEdges(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		for (LinkType type:LinkType.values())
			bldr.addOutkey(type.ordinal());
		bldr.addValues(addparam("STATUS","OK"));
		return bldr.build();
	}

	//returns attr ids of a node
	private FggDataServiceOuterClass.FggMsg onGetAttrs(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		for (int fmk:FieldMeta.getAttrKeys(getParam(request, "isnode").equals("true"), new HashSet<Integer>()))
			bldr.addOutkey(fmk);
		bldr.addValues(addparam("STATUS","OK"));
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onGetNodeInfo(FggDataServiceOuterClass.FggMsg request)
	{
		int pos = 0;
		String str = null;
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);

		int nodekey = getParamInt(request, "nodekey");
		CBOType type = CBOType.valueOf(nodekey);
		if (type != null)
		{
			bldr.addValues(addparam("name",type.name()));
			bldr.addValues(addparam("rootnodekey",   ""+type.root()));
			bldr.addValues(addparam("legnodekey",    ""+type.leg()));
			bldr.addValues(addparam("parentnodekey", ""+type.parent()));
			bldr.addValues(addparam("STATUS","OK"));
		}
		else bldr.addValues(addparam("STATUS","Invalid node"));
		return bldr.build();
	}

	//returns all edges with parent and child node connections
	private FggDataServiceOuterClass.FggMsg onGetEdgeInfo(FggDataServiceOuterClass.FggMsg request) {
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		int edge = getParamInt(request, "edgekey");
		LinkType type = LinkType.valueOf(edge);
		if (type == null) {
			bldr.addValues(addparam("STATUS","Invalid edge"));
		} else {
			bldr.addValues(addparam("name",type.name()));
			for (int i=0;i<type.maxnodes();i++)
				bldr.addValues(addparam("nodekey"+i, ""+type.nodekey(i)));
			bldr.addValues(addparam("STATUS","OK"));
		}
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onGetAttrInfo(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		FieldMeta meta = FieldMeta.lookup(getParamInt(request, "attrkey"));
		if (meta == null) {
			bldr.addValues(addparam("STATUS","Invalid Attr"));
		} else {
			bldr.addValues(addparam("name",meta.fname()));
			bldr.addValues(addparam("dtype",meta.dtype().toString()));
			bldr.addValues(addparam("isnode",""+meta.isNode()));
			bldr.addValues(addparam("key",""+meta.key()));
			bldr.addValues(addparam("splitbyleg",""+meta.splitByLeg()));
			bldr.addValues(addparam("STATUS","OK"));
		}
		return bldr.build();
	}

	//Get by mathing key or get all keys
	private FggDataServiceOuterClass.FggMsg onGetObjKeys(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		CBOType type = CBOType.valueOf(getParamInt(request, "nodekey"));
		String matchkey = getParam(request, "matchkey");

		Map<Integer,Integer> objkeys = Cache.getObjectKey(type, matchkey, new HashMap<Integer,Integer>());
		if (matchkey == null || matchkey.length() == 0)
			objkeys.forEach((k, v) -> bldr.addOutkey(k));
		else
			objkeys.forEach((k, v) -> bldr.addValues(addparam(k+"",v+"")));
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onSetObjKey(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		CBOType type 	= CBOType.valueOf(getParamInt(request, "nodekey"));
		int objkey 		= getParamInt(request, "objkey");
		int altkeynum 	= getParamInt(request, "altkeyseq");
		if (type == null)
			bldr.addValues(addparam("STATUS","Invalid Attr"));
		else
			bldr.addOutkey(Cache.setObjectKey(type, altkeynum, objkey, getParam(request, "str_key")));
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onSetLinkKey(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		LinkType type 	= LinkType.valueOf(getParamInt(request, "edgekey"));
		int linkkey = 0;
		if (type == null) {
			bldr.addValues(addparam("STATUS","Invalid edgekey"));
		} else {
			int[] objkeys = new int[type.maxnodes()];
			for (int i=0;i<objkeys.length;i++)
				objkeys[i] = getParamInt(request, "objkey"+i);
			linkkey = type.index().getLink(objkeys);
			if (linkkey <= 0)
				linkkey = Cache.addLink(type, objkeys,
					getParamInt(request, "fromdt"),
					getParamInt(request, "todt"));
			if (linkkey <= 0)
				bldr.addValues(addparam("STATUS","Invalid objkey"));
			else
				bldr.addOutkey(linkkey);
		}
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onGetLinkKeys(FggDataServiceOuterClass.FggMsg request)
	{
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		LinkType type 	= LinkType.valueOf(getParamInt(request, "edgekey"));
		if (type == null) {
			bldr.addValues(addparam("STATUS","Invalid edgekey"));
		} else {
			int[] objkeys = new int[type.maxnodes()];
			for (int i=0;i<objkeys.length;i++)
				objkeys[i] = getParamInt(request, "objkey"+i);
			Set<Integer> links = type.index().findLink(objkeys, new HashSet<Integer>());
			links.forEach((k) -> bldr.addOutkey(k));
		}
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onTaskRequest(FggDataServiceOuterClass.FggMsg request){
		return null;
	}

	private FggDataServiceOuterClass.FggMsg onAddAttr(FggDataServiceOuterClass.FggMsg request)
    {
		FggDataServiceOuterClass.FggMsg.Builder bldr = create(request);
		FieldType   ftype = FieldType.valueOf(getParam(request, "fieldtype"));
		DataType    dtype = DataType.valueOf(getParam(request, "datatype"));
        CBOType     ntype = CBOType.valueOf(getParamInt(request, "nodeid"));
        int     attrsize  = getParamInt(request, "attrsize");
        String  attrname  = getParam(request, "attrname").toLowerCase();
        FieldMeta fld = (ntype == null)? null:FieldMeta.lookup(ntype, attrname);
        if (fld == null) {
            if (attrsize > 0 && dtype != null && ftype != null)
            {
                try {
                    List<CBO> cbos = new ArrayList<CBO>();
                    Map<String,String> prop = CBOBuilder.getAttrProperties(dtype == DataType.ENUM);
                    cbos.add(CBOBuilder.createAttr(ntype, attrname, dtype, ftype, attrsize, prop));
                    CBOBuilder.persist(CBOBuilder.Table.ATTR, cbos);
                    FieldMeta.fromCBO(cbos.get(0));
                    bldr.addOutkey(cbos.get(0).recid());
                } catch (Exception e) {
                    bldr.addValues(addparam("STATUS","Process error"));
                }
            }
            else
                bldr.addValues(addparam("STATUS","Invalid params"));
        }
        else {
            bldr.addValues(addparam("STATUS","Duplicate attrname"));
        }
		return bldr.build();
	}

	private FggDataServiceOuterClass.FggMsg onNotifyGitCheckin(FggDataServiceOuterClass.FggMsg request){
		return null;
	}

	private FggDataServiceOuterClass.FggMsg onNotifyCBORefresh(FggDataServiceOuterClass.FggMsg request){
		return null;
	}


	@Override
    //node,k1,attr[]
	public void requestData(FggDataServiceOuterClass.FggMsg request,
		StreamObserver<FggDataServiceOuterClass.FggData> responseObserver)
	{
        //GET_OBJECT
        //System.out.println(request);
		int instkey = getParamInt(request, "instkey");
        int typekey = getParamInt(request,  "typekey");

		CBOType  ctype  = CBOType.valueOf(typekey);
		LinkType ltype 	= LinkType.valueOf(getParamInt(request, "edgekey"));

        if (ltype != null && ctype != null)
            instkey = ltype.index().objkey(instkey, ctype);

        if (ctype == null || !ctype.recorder.has(instkey))
        {
            responseObserver.onCompleted();
            return;
        }

        Record rec = null;
        List<FieldMeta> attrkeys = new ArrayList<FieldMeta>();
        int i=0;
        while (true) {
            FieldMeta m = FieldMeta.lookup(getParamInt(request, "attrkey"+ (i++)));
            if (m == null) break;
            if (typekey != m.key()) continue;
            attrkeys.add(m);
        }

		final List<FeatureData> datalist = new ArrayList<FeatureData>();
        for (FieldMeta m:attrkeys) {
			if (m.isderived()) {

			} else {
				if (rec == null) rec = Cache.getRecord(typekey,instkey);
				FeatureData data = new FeatureData();
                data.setObjKey(instkey);
	            data.copy(rec, m);
                //System.out.println(m + "\n" + rec + "\n" + data + "\n----------");
	            datalist.add(data);
			}
        }

		// You must use a builder to construct a new Proto buffer object
		//FggDataServiceOuterClass.FggData response =
		//	FggDataServiceOuterClass.FggData.newBuilder()
		//		.setStrValue("Hello there, " + request.getRequest())
		//		.build();
		CallStreamObserver<FggDataServiceOuterClass.FggData> cso =
			(CallStreamObserver<FggDataServiceOuterClass.FggData>)responseObserver;

		//Use responseObserver to send a single response back
		for (FeatureData f:datalist)
		{
			for (FggDataServiceOuterClass.FggData d:f.xmits())
			{
				//Sleep for slow client
				while (!cso.isReady())
					try { Thread.sleep(100); } catch (Exception e) { }
				cso.onNext(d);
			}
		}
		// When you are done, you must call onCompleted.
		responseObserver.onCompleted();
	}

	@Override
    public StreamObserver<FggDataServiceOuterClass.FggData> persistData(
						final StreamObserver<FggDataServiceOuterClass.FggMsg> responseObserver)
	{
		//On complete, take the entire datalist and handover to a worker
		final List<FeatureData> datalist = new ArrayList<FeatureData>();
		return new StreamObserver<FggDataServiceOuterClass.FggData>()
		{
			FeatureData feature = null;

			@Override
			public void onNext(FggDataServiceOuterClass.FggData data)
			{
				if (feature == null)
					feature = new FeatureData();

				feature.add(data);

				if (feature.isFullXmit())
				{
					datalist.add(feature);
					feature = null;
				}
			}

			@Override
			public void onError(Throwable t) {
				//send status for a single message
				t.printStackTrace();
				FggDataServiceOuterClass.FggMsg.Builder msg = FggDataServiceOuterClass.FggMsg.newBuilder().setRequest(
																MsgType.STATUS.ordinal());
				msg.addValues(addparam("MSG","INCOMPLETE"));
				responseObserver.onNext(msg.build());
				responseObserver.onCompleted();
			}

			@Override
			public void onCompleted()
			{
				FggDataServiceOuterClass.FggMsg.Builder msg = FggDataServiceOuterClass.FggMsg.newBuilder().setRequest(
																MsgType.STATUS.ordinal());

				if (feature != null) {
					msg.addValues(addparam("MSG","INCOMPLETE"));
				} else {
					int count = 0;
					//persist datalist
					Map<Long,Record> records = new HashMap<Long,Record>();

					for (FeatureData f:datalist)
					{
						FieldMeta m = FieldMeta.lookup(f.getAttrKey());
						long lkey = (f.key() * 1000000) + m.key();

						Record rec = records.get(lkey);
						if (rec == null)
						{
							rec = Cache.getRecord(m.key(), f.key());
                            //System.out.println(f);
							if (rec != null) records.put(lkey, rec);
						}
						if (rec == null || m == null) continue;
						f.copy2rec(rec,m);
						count++;
					}
					records.forEach( (k,v) -> Cache.setRecord((int)(k%1000000),v));
					records.clear();
					msg.addValues(addparam("RECEIVED",""+datalist.size()));
					msg.addValues(addparam("UPDATED",""+count));
				}
				responseObserver.onNext(msg.build());
				responseObserver.onCompleted();
			}
		};
	}

	/////////////////////////////////////// HELPERS ///////////////////////////////////////////
	private static FggDataServiceOuterClass.params addparam(String name, String val) {
		return FggDataServiceOuterClass.params.newBuilder().setName(name).setValue(val).build();
	}

	private static FggDataServiceOuterClass.FggMsg.Builder create(FggDataServiceOuterClass.FggMsg msg) {
		return FggDataServiceOuterClass.FggMsg.newBuilder(msg);
	}

	private static int getParamInt(FggDataServiceOuterClass.FggMsg request, String paramname) {
		String val = getParam(request,paramname);
		return (val==null||val.length()==0)? -1:Integer.parseInt(val);
	}

	private static String getParam(FggDataServiceOuterClass.FggMsg request, String paramname) {
		String paramid = null;
		for (FggDataServiceOuterClass.params param:request.getValuesList())
			if (param.getName().equals(paramname))
				paramid = param.getValue();
		return paramid;
	}

}
