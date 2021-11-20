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

public class FggTest
{
	public static void main1( String[] args ) throws Exception
    {
        CBOBuilder.buildAll();
        FieldMeta m = FieldMeta.lookup(CBOType.valueOf("CUSTOMER"), "cust_key");
        Field f = new Field(m);
        f.seto(20150101, null);
        f.seto(20160101, null);
        f.seto(20170101, null);
        f.seto(20180101, null);
        System.out.println(f);
    }

	public static void main( String[] args ) throws Exception
	{
			Cache.init();
			//String expr = "household->customer->deposit($principal_ending_bal = 10)->$principal_gl_ending_bal->sum()";
			String expr = "customer(($emp_flg='N')&($person_org_flg='Org'))->$person_org_flg->count()";
			PathExpr pexpr = new PathExpr(expr);
			List<Object> res = pexpr.calculate(CBOType.valueOf("CUSTOMER").recorder, 20190601, new ArrayList<Object>());
			//System.out.println(CBOType.valueOf("CUSTOMER").recorder.get(150));
			//res.forEach((v)->System.out.println(v));
			IFunction count = IFunction.getFunction("count");
			count.clear();
			res.forEach((v)->count.process(v,Integer.parseInt(v.toString())));
			System.out.println(count.geto());
	}

}
