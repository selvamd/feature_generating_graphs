package fgg.access;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.*;
import fgg.data.*;
import fgg.utils.*;

//Recorder implementation with index for faster loading
public class FileIndexRecorder extends Recorder
{
	private String filename;
	private List<FieldMeta> metas;
	private RandomAccessFile filestore;
	private int maxdate = 0;

	final ElasticMap strkeymap;
	final LinkIndex linkindex;

	//If present in both, memory is the latest
	Map<Long,Long>   stored; //stored in disk

	//Only main recorders have cache, not bigrecorders
	//Cache is used to buffer old objects during updates
	Map<Long,Record>    cached;

	private FileIndexRecorder(String filename, List<FieldMeta> meta,
                            ElasticMap keymap, LinkIndex index)
	{
		this.filename 	= filename;
		this.metas    	= meta;
		this.strkeymap 	= keymap;
        this.linkindex 	= index;
        this.stored   	= new ConcurrentHashMap<Long,Long>();
        this.cached   	= new ConcurrentHashMap<Long,Record>();
	}

	public Map<Integer,Integer> findKey(String val, Map<Integer,Integer> result) {
		return strkeymap.find(val,result);
	}

	public void storeKey(int recid, int altkeynum, String val)
	{
		Record rec = get(recid);
		if (rec == null) rec = newrecord(recid);
		if (rec.storeKey(metas, altkeynum, val))
			strkeymap.map(val, recid, altkeynum);
		add(rec);
	}

	public void print() {
		for (long i:stored.keySet())
			System.out.println(get(i));
	}


	public void add(Record rec)
	{
		if (cached.get(rec.recordid()) != null)
			throw new ConcurrentModificationException("Another process updating cache");
        store(rec, stored.containsKey(rec.recordid())? stored.get(rec.recordid()):-1);
	}

	public void delete(long key)
	{
		if (!stored.containsKey(key)) return;
		stored.remove(key);
	}

	public int size() {
		return stored.size();
	}

	public int maxdate() {
		return this.maxdate;
	}

	public boolean has(long recid)
	{
        return stored.containsKey(recid);
	}

	public Set<Long> keys(Set<Long> keys)
	{
        keys.clear();
		keys.addAll(stored.keySet());
		return keys;
	}

	//uncache function is called after record updates in another process ends
	public void uncache(long recid, int pos, int size)
	{
		if (cached.containsKey(recid))
			cached.remove(recid);
	}

	//cache function is called before record updates in another process begins
	public Record cache(long recid)
	{
		Record rec = get(recid);
		cached.put(recid, rec);
		return rec;
	}

    /*
    public Record findOrCreateAndCache(int recid, int flushsize) 
    {
		if (cached == null) return null;
		
        //Return cache record if already there
        if (cached.containsKey(recid))
            return cached.get(recid);
		
        //if new entry but cache is already too big, flush first
        if (cached.size() > flushsize) 
        {
            cached.forEach((k,v)->add(v));
            cached.clear();
        }
		
        //find or create
        Record rec = cache(recid);
        if (rec == null)
            cached.put(recid, rec = newrecord(recid));
        return rec;
    } */


	public Record get(long recid)
	{
		Record rec = cached.get(recid);
		if (rec != null) return rec;
		if (stored.containsKey(recid))
			return load(stored.get(recid));
		return null;
	}

	public static Recorder make(String name, List<FieldMeta> meta, ElasticMap map, LinkIndex index)
	{
		FileIndexRecorder recorder = new FileIndexRecorder(name, meta, map, index);
		recorder.initFileStore();
		return recorder;
	}

	public Record newrecord(long id)
	{
		Record rec = new Record(id);
		for (FieldMeta m: metas)
			rec.getOrAddField(m);
		return rec;
	}

	public void close()
	{
		try {
			filestore.close();
            storeIndex();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//////////////////  DISK ACCESS FUNCTIONS ///////////////////////
	private void initFileStore()
	{
		try {
			filestore = new RandomAccessFile(Persistor.filestorepath() + this.filename + ".ser", "rw");
            loadIndex();
            /*
              int[] objs = (linkindex == null)? null:new int[linkindex.type().maxnodes()];
			for (long l:this.stored.keySet())
			{
				Record rec = load(this.stored.get(l));
                this.maxdate = Math.max(maxdate, rec.maxdate(0));
                if (strkeymap != null)
                {
                    int count = 0;
                    String val = null;
                    while ((val = rec.getObjStrKey(metas, ++count))!= null)
                        strkeymap.map(val, (int)rec.recordid(), count);
                }
                if (linkindex != null) {

                    Field f = rec.field("isvalid");
                    int from = -1, to = -1;
                    rec.getLinkObjs(linkindex.type(), objs);
                    for (int dt = f.mindt(); dt < 99999999; dt = f.scddt(dt))
                    {
                        if (1 == (int)f.getd(dt)) from = dt;
                        if (0 == (int)f.getd(dt)) to = dt;
                        if (from < 0 || to < 0) continue;
                        linkindex.loadLink((int)rec.recordid(), objs, from, to);
                        from = to = -1;
                    }
                    if (from > 0) linkindex.loadLink((int)rec.recordid(), objs, from, 99999999);
                }
			}*/
			System.out.println("Objects for " + filename + " loaded = " + size());
		} catch (Exception e) {
            e.printStackTrace();
		}
	}

	private Record load(long pos)
	{
		try {
			byte[] b = new byte[Utils.lsb(pos)];
			synchronized (this) 
			{
				this.filestore.seek(lpos(pos));
				this.filestore.readFully(b);
			}
			ByteBuffer buff = ByteBuffer.wrap(b);
			Record rec = new Record();
			rec.deserialize(buff, metas);
			for (FieldMeta m: metas)
				rec.addField(m);
			return rec;
		} catch (Exception e) {
			System.out.println("Pos value = " + pos + ","+ Utils.msb(pos)+ ":"+Utils.lsb(pos));
			e.printStackTrace();
		}
		return null;
	}

	private void store(Record rec, long pos)
	{
		try {
            int len = rec.length();
			int buflen = (len/5000 + 1) * 5000;
            ByteBuffer buff = ByteBuffer.allocate(buflen);
			rec.serialize(buff);
			synchronized (this) 
			{ 
				long lpos = (pos < 0 || Utils.lsb(pos)/5000 == len/5000)? lpos(pos):lpos(-1);
				this.filestore.seek(lpos);
				this.filestore.write(buff.array(), 0, buflen);
				this.stored.put(rec.recordid(), Utils.makelong((int)(lpos/5000), len));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private long lpos(long pos) throws IOException {
		return (pos >= 0)? (5000l * Utils.msb(pos)):
			(this.filestore.length()/5000l)*5000l;
	}

    //called when new links are created to append to file
    private void storeIndex()
    {
    	try {
    		DataOutputStream outstream = 
				new DataOutputStream(
					new BufferedOutputStream(
						new FileOutputStream(
							Persistor.filestorepath() + this.filename + ".idx")));
							
            Map<Integer,int[]> map = (strkeymap == null)? null:strkeymap.remapByRecId();
            SortedMap<Integer,Integer> times = new TreeMap<Integer,Integer>();
            for (Map.Entry<Long,Long> ent:this.stored.entrySet()) 
			{
                outstream.writeLong(ent.getKey());
                outstream.writeLong(ent.getValue());
                if (map == null) 
                {
                    outstream.writeInt(linkindex.type().maxnodes());
                    for (int i=0;i<linkindex.type().maxnodes();i++) {
                        CBOType typ = CBOType.valueOf(linkindex.type().nodekey(i));
                        outstream.writeInt(linkindex.objkey((int)ent.getKey().longValue(), typ));
                    }
                    linkindex.getTimes((int)ent.getKey().longValue(), times);
                    outstream.writeInt(times.size());
                    for (Map.Entry<Integer,Integer> tent:times.entrySet()) {
                        outstream.writeInt(tent.getKey());
                        outstream.writeInt(tent.getValue());
                    }
                } else {
                    int[] sok = map.get((int)ent.getKey().longValue());
                    outstream.writeInt(sok.length);
                    for (int i:sok) outstream.writeInt(i);
                }
            }
    		outstream.flush();
			outstream.close();
    	} catch (Exception e) {
            e.printStackTrace();
    	}
    }

    //Loads valid links from file system during construction
    private void loadIndex() {
        int totalrecs = 0;
    	try {
    		DataInputStream stream = 
				new DataInputStream(
					new BufferedInputStream(
						new FileInputStream(
							Persistor.filestorepath() + this.filename + ".idx")));
            
    		while(stream.available() > 0) {
                long recid = stream.readLong();
                this.stored.put(recid,stream.readLong());
                int[] sok = new int[stream.readInt()];
                for (int i=0;i<sok.length;i++)
                    sok[i] = stream.readInt();
                if (strkeymap == null)
                {
                    int size = stream.readInt();
                    totalrecs += size;
                    for (int i=0;i<size;i++) 
                        linkindex.loadLink((int)recid, sok, stream.readInt(), stream.readInt());
                } 
                else 
                {
                    for (int i=0;i<sok.length;i++) 
                    { 
                        if (sok[i] == 0) continue;
                        strkeymap.map(sok[i], (int)recid, i+1);
                    }
                    totalrecs += sok.length;
                }                    
            }
    		stream.close();
    	} catch (FileNotFoundException fe) {
    	} catch (Exception e) {
            e.printStackTrace();
    	}
        //System.out.println("Loaded " + totalrecs + " indices  for " + filename);
    }

}
