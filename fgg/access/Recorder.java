package fgg.access;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;
import java.nio.*;
import fgg.data.*;
import fgg.utils.*;


public abstract class Recorder
{
	public abstract Map<Integer,Integer> findKey(String val, Map<Integer,Integer> result);
	public abstract void storeKey(int recid, int altkeynum, String val);
	public abstract void add(Record rec);
	public abstract void delete(long key);
	public abstract boolean has(long recid);
	public abstract Set<Long> keys(Set<Long> keys);
	public abstract Record get(long recid);
	public abstract Record newrecord(long id);
	public abstract void close();
}
