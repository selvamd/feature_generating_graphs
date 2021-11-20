package fgg.utils;

import java.util.*;
import fgg.data.*;
import fgg.access.*;

//Example household
//          ->customer($ptype='business')
//              ->deposit(($principal_gl_ending_bal > 10000) & ($type='dd'))
//                  ->$principal_gl_ending_bal [$rec for the whole record]
//                      ->sum()
public class PathExpr
{
    public final LinkType[] links;
    public final Expr[] exprs;
    public final CBOType[] ctypes;
    public final FieldMeta attr;
    public final IFunction func;

    public PathExpr(String expr) throws Exception
    {
        List<String> arr = new ArrayList<String>();
        while (expr.indexOf("->") > 0) {
            arr.add(expr.substring(0, expr.indexOf("->")));
            expr = expr.substring(2+expr.indexOf("->"));
        }

        func = IFunction.getFunction(expr.substring(0,expr.length()-2));
        if (func == null) throw new Exception("Bad reducer function");

        int size = arr.size();
        if (!arr.get(size-1).startsWith("$"))
            throw new Exception("Bad target attr");

        links  = new LinkType[size-2];
        exprs  = new Expr[size-1];
        ctypes = new CBOType[size-1];

        String init = arr.get(0);
        init = (!init.endsWith(")"))? init:init.substring(0, init.indexOf("("));
        ctypes[0] = CBOType.valueOf(init);

        for (int i=0;i<links.length;i++)
        {
            String cn = arr.get(i+1);
            cn = (!cn.endsWith(")"))? cn:cn.substring(0, cn.indexOf("("));
            links[i] = LinkType.valueOf(cn);
            if (links[i] == null)
            {
                String pn = arr.get(i);
                pn = (!pn.endsWith(")"))? pn:pn.substring(0, pn.indexOf("("));
                links[i] = LinkType.valueOf(CBOType.valueOf(pn),CBOType.valueOf(cn));
            }
            if (links[i] == null)
                throw new Exception("Invalid link in the path expr");
            if (links[i].maxnodes() != 2)
                throw new Exception("Invalid link in the path expr");
        }

        for (int i=0;i<exprs.length;i++)
        {
            String pn = arr.get(i);
            if (!pn.endsWith(")")) continue;
            exprs[i] = Expr.getExpr(pn.substring(pn.indexOf("(")));
            if (exprs[i] == null)
                throw new Exception("Invalid filter in the path expr");
        }

        Set<String> fields = new HashSet<String>();
        for (int i=0;i<=links.length;i++)
        {
            if (exprs[i] != null)
            {
                for (String fld:exprs[i].getVars(fields))
                    if (FieldMeta.lookup(ctypes[i],fld) == null)
                        throw new Exception("Invalid filter fields");
                if (i == links.length) break;
            }
            if (links.length == 0) continue;
            ctypes[i+1] = CBOType.valueOf((links[i].nodekey(0) == ctypes[i].ordinal())?
                    links[i].nodekey(1):links[i].nodekey(0));
        }

        attr = FieldMeta.lookup(ctypes[links.length],arr.get(size-1).substring(1));
        if (attr == null && !("$rec".equals(arr.get(size-1))))
            throw new Exception("Invalid attr in the path expr");
    }

    //Returns a derived value for a single reckey and asofdt
    public Object calculateDt(int reckey, Record rec, int asofdt) {
        func.clear();
        _calculateDt(reckey, rec, asofdt, 0,
            loadRecords(reckey, rec, 0, new HashMap<Long,Record>()));
        return func.geto();
    }

    //Returns all derived value for all reckeys and single asofdt in recorder's key order
    public List<Object> calculate(Recorder rec, int asofdt, List<Object> out)
    {
        out.clear();
        Map<Long,Record> recmap = new HashMap<Long,Record>();
        for (long k:rec.keys(new TreeSet<Long>()))
        {
            recmap.clear();
            func.clear();
            loadRecords((int)k, null, 0, recmap);
            int visited = _calculateDt((int)k, null, asofdt, 0, recmap);
            if (visited > 0) out.add(func.geto());
        }
        return out;
    }

    //Returns all derived value for single reckey across all dates
    public SortedMap<Integer,Object> calculate(int reckey, Record rec, SortedMap<Integer,Object> out)
    {
        out.clear();
        //pre-laods all the required records (only diskIO component)
        Map<Long,Record> recmap = loadRecords(reckey, rec, 0, new HashMap<Long,Record>());
        //Get updated dates for all input variables and recompute on all those dates
        findscddates(recmap, new TreeSet<Integer>()).forEach(
            dt-> { func.clear();_calculateDt(reckey,rec,dt,0,recmap);out.put(dt,func.geto()); });
        return out;
    }

    //Private method for all calculates which returns number of objects visited by the reducer
    //function after all the filters are applied
    private int _calculateDt(int reckey, Record rec, int asofdt, int idx, Map<Long,Record> recmap)
    {
        int visited = 0;
        LinkIndex index = (links.length==0)? null:links[idx].index();
        if (rec == null) rec = recmap.get(Utils.makelong(ctypes[idx].ordinal(),reckey));
        if (exprs[idx] != null && !exprs[idx].filter(rec, asofdt)) return 0;
        if (idx >= links.length)
        {
            //update reducer
            if (attr != null) {
                Field f = rec.field(attr);
                func.process(f.geto(asofdt),f.getd(asofdt));
            } else func.process(rec,-1);
            return 1;
        } else {
            int objidx = (links[idx].nodekey(0) == ctypes[idx].ordinal())? 0:1;
            int[] objs = new int[2];objs[0] = objs[1] = 0;objs[objidx] = reckey;
            for (int linkkey:index.findLink(objs, new HashSet<Integer>()))
            {
                if ((linkkey = index.filterByTime(linkkey,asofdt)) <= 0) continue;
                visited += _calculateDt(index.objkey(linkkey,ctypes[idx+1]), null, asofdt, 1+idx, recmap);
            }
        }
        return visited;
    }

    private SortedSet<Integer> findscddates(Map<Long,Record> recmap, SortedSet<Integer> out)
    {
        out.clear();
        Set<String> fields = new HashSet<String>();
        for (Long l:recmap.keySet())
        {
            Record rec = recmap.get(l);
            int node = Utils.msb(l.longValue());
            for (int i=0;i<ctypes.length;i++) {
                if (ctypes[i].ordinal() != node) continue;
                if (exprs[i] == null || i != ctypes.length-1) continue;
                //Get the change dates on reduced attr
                if (i == ctypes.length-1)
                {
                    Field f = rec.field(attr);
                    for (int dt=f.mindt();dt<99999999;dt=f.nextScdDate(dt))
                        out.add(dt);
                }
                if (exprs[i] == null) continue;
                //Get the change dates on filtering attrs
                for (String fld:exprs[i].getVars(fields)) {
                    Field f = rec.field(fld);
                    for (int dt=f.mindt();dt<99999999;dt=f.nextScdDate(dt))
                        out.add(dt);
                }
            }
        }
        return out;
    }

    //recursively loads all records in the dependendcy tree for the evaluation
    private Map<Long,Record> loadRecords(int reckey, Record rec, int idx, Map<Long,Record> recs)
    {
        if (reckey == -1) return recs;
        //Load the record from disk to either eval an expr or get final data for reduction
        if (idx >= links.length || exprs[idx] != null)
        {
            if (rec == null) rec = ctypes[idx].recorder.get(reckey);
            recs.put(Utils.makelong(ctypes[idx].ordinal(),reckey), rec);
            if (idx >= links.length) return recs; //recursion terminator
        }

        int[] objs = new int[2];
        LinkIndex index = links[idx].index();
        int objidx = (links[idx].nodekey(0) == ctypes[idx].ordinal())? 0:1;
        objs[0] = objs[1] = 0;objs[objidx] = reckey;
        for (int linkkey:index.findLink(objs, new HashSet<Integer>()))
            loadRecords(index.objkey(linkkey,ctypes[idx+1]), null, idx+1, recs);
        return recs;
    }

    public String toString() {
        String str = "Links=";
        for (LinkType link:links) str += link + ",";
        str += "\nCBOType=\n";
        for (int i=0;i<ctypes.length;i++)
        {
            str += "\t"+ctypes[i];
            str += ",filters=";
            str += (exprs[i] == null)? "[]":exprs[i].getVars(new HashSet<String>());
            str += ",\n";
        }
        str += "Attr=" + attr.fname();
        return str;
    }
}
