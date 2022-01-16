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
	public final int   request;
	//public final CBOType   cbo;
	//public final LinkType link;
	//public final String   expr;

	public FggRequest(CBOType type) {
		request = REQUESTID++;
	}
	//nodeattr + edgeattr
	//getLinks(linktype, objkeys, asofdt, bIncludeObj) -> []
	//getObjectKey(cbotype, strkey, strexpr, expr_evaldt) -> []
}
