package fgg.access;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.*;
import fgg.data.*;
import fgg.utils.*;


public class FileRecorder extends Recorder
{
	private int recsize;
	private String filename;
	private List<FieldMeta> metas;
	private RandomAccessFile filestore;
	private FileRecorder bigrecorder;
	private long failedinserts = 0;
	private int maxdate = 0;

	final ElasticMap strkeymap;
	final LinkIndex index;

	//If present in both, memory is the latest
	Map<Long,Integer>   stored; //stored in disk

	//stores pos of removed record during delete or update operations
	Map<Long,Integer>   removed;

	//Only main recorders have cache, not bigrecorders
	//Cache is used to buffer old objects during updates
	Map<Long,Record>    cached;

	private FileRecorder(String filename, int initsize, List<FieldMeta> meta, ElasticMap keymap, LinkIndex index)
	{
		this.recsize  = initsize;
		this.filename = filename;
		this.stored   = new HashMap<Long,Integer>();
		this.removed  = new HashMap<Long,Integer>();
		this.metas    = meta;
		this.strkeymap = keymap;
        this.index = index;
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


	public synchronized void add(Record rec)
	{

		long key = rec.recordid();
		int len = 4 + rec.length();

		if (cached != null && (cached.get(key)) != null)
			throw new ConcurrentModificationException("Another process updating cache");

		if (len >= recsize)
		{
			delete(key);
			if (bigrecorder == null)
			{
				bigrecorder = new FileRecorder(filename, recsize * 2, metas, strkeymap, index);
				bigrecorder.initFileStore();
			}
			bigrecorder.add(rec);
			return;
		}

		if (removed.containsKey(key))
		{
			int pos = removed.get(key);
			store(rec, pos);
			removed.remove(key);
			stored.put(key,pos);
		}
		else if (stored.containsKey(key))
			store(rec, stored.get(key));
		else  {
			int pos = store(rec, -1);
			if (pos >= 0)
				stored.put(rec.recordid(), pos);
		}
	}

	public synchronized void delete(long key)
	{
		if (bigrecorder != null)
			bigrecorder.delete(key);

		if (stored.containsKey(key))
		{
			erase(stored.get(key));
			removed.put(key, stored.get(key));
			stored.remove(key);
		}
	}

	public synchronized int size() {
		int size = stored.size();
		if (bigrecorder != null)
			size += bigrecorder.size();
		return size;
	}

	public int maxdate() {
		return (bigrecorder == null)? this.maxdate:
			 Math.max(this.maxdate,bigrecorder.maxdate());
	}

	public synchronized boolean has(long recid)
	{
		if (stored.containsKey(recid)) return true;
		if (bigrecorder != null && bigrecorder.has(recid)) return true;
		return false;
	}

	public synchronized Set<Long> keys(Set<Long> keys)
	{
		keys.addAll(stored.keySet());
		if (bigrecorder != null)
			bigrecorder.keys(keys);
		return keys;
	}

	//uncache function is called after record updates in another process ends
	public synchronized void uncache(long recid, int pos, int size)
	{
		if (cached != null && cached.containsKey(recid))
			cached.remove(recid);

		int len = 4 + size;
		if (len >= recsize)
		{
			removed.put(recid, stored.get(recid));
			stored.remove(recid);

			if (bigrecorder == null)
			{
				bigrecorder = new FileRecorder(filename, recsize * 2, metas, strkeymap, index);
				bigrecorder.initFileStore();
			}
			bigrecorder.uncache(recid, pos, size);
		} else
			stored.put(recid, pos);
	}

	public synchronized int pos(long recid)
	{
		if (stored.containsKey(recid))
			return stored.get(recid);
		else
			return (bigrecorder == null)? -1:bigrecorder.pos(recid);
	}

	//cache function is called before record updates in another process begins
	public synchronized Record cache(long recid)
	{
		if (cached == null) return null;
		Record rec = get(recid);
		cached.put(recid, rec);
		return rec;
	}


	public synchronized Record get(long recid)
	{
		Record rec = null;
		if (cached != null && (rec = cached.get(recid)) != null)
			return rec;
		if (bigrecorder != null && (rec = bigrecorder.get(recid)) != null)
			return rec;
		if (stored.containsKey(recid))
			return load(stored.get(recid));
		return null;
	}

	public static Recorder make(String name, List<FieldMeta> meta, ElasticMap map, LinkIndex index)
	{
		int size = 5000;
		FileRecorder recorder = new FileRecorder(name, size, meta, map, index);
		recorder.cached = new HashMap<Long,Record>();
		FileRecorder next = recorder;
		next.initFileStore();

		while (next.size() > 0)
		{
			System.out.println("Objects for " + name + " with maxsize " + size + "  loaded = " + next.size());
			size = size * 2;
			next.bigrecorder = new FileRecorder(name, size, meta, recorder.strkeymap, recorder.index);
			next = next.bigrecorder;
			next.initFileStore();
		}
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
			this.filestore.close();
			if (this.bigrecorder != null)
				bigrecorder.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	//////////////////  DISK ACCESS FUNCTIONS ///////////////////////
	private void initFileStore()
	{
		try {
			filestore = new RandomAccessFile(Persistor.filestorepath() + this.filename + "." + this.recsize + ".ser", "rw");
			int len = (int)(this.filestore.length() / (long)this.recsize);
            int[] objs = (index == null)? null:new int[index.type().maxnodes()];
			for (int i=0; i < len; i++)
			{
				Record rec = load(i);
				if (rec != null) {
                    //System.out.println(rec);
                    //Thread.sleep(10000);
					this.stored.put(rec.recordid(), i);
					this.maxdate = Math.max(maxdate, rec.maxdate(0));
                    if (strkeymap != null)
                    {
                        int count = 0;
                        String val = null;
                        while ((val = rec.getObjStrKey(metas, ++count))!= null)
                            strkeymap.map(val, (int)rec.recordid(), count);
                    }
                    if (index != null) {

                        Field f = rec.field("isvalid");
                        int from = -1, to = -1;
                        rec.getLinkObjs(index.type(), objs);
                        for (int dt = f.mindt(); dt < 99999999; dt = f.nextScdDate(dt))
                        {
                            if (1 == (int)f.getd(dt)) from = dt;
                            if (0 == (int)f.getd(dt)) to = dt;
                            if (from < 0 || to < 0) continue;
                            index.loadLink((int)rec.recordid(), objs, from, to);
                            from = to = -1;
                        }
                        if (from > 0) index.loadLink((int)rec.recordid(), objs, from, 99999999);
                    }
				}
			}
		} catch (Exception e) {
            e.printStackTrace();
		}
	}

	private void erase(int pos)
	{
		try {
			long lpos = (long)pos*(long)recsize;
			this.filestore.seek(lpos);
			this.filestore.writeInt(0);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Record load(int pos)
	{
		try {
			this.filestore.seek(lpos(pos));
			int size = this.filestore.readInt();
			if (size == 0) return null;
			byte[] b = new byte[recsize-4];
			this.filestore.readFully(b);
			ByteBuffer buff = ByteBuffer.wrap(b);
			Record rec = new Record();
			for (FieldMeta m: metas)
				rec.addField(m);
			rec.deserialize(buff, metas);
			return rec;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	private int store(Record rec, int pos)
	{
		try {
			long lpos = lpos(pos);
			this.filestore.seek(lpos);
			this.filestore.writeInt(rec.length());
			ByteBuffer buff = ByteBuffer.allocate(recsize-4);
			rec.serialize(buff);
			this.filestore.write(buff.array(), 0, recsize-4);
			return (int)(lpos/recsize);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	private long lpos(int pos) throws IOException {
		return (pos < 0)?
			(this.filestore.length()/recsize)*recsize:
			(long)pos*(long)recsize;
	}


}
