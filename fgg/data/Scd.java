package fgg.data;

import java.util.*;
import java.io.*;
import java.nio.*;

public class Scd implements Serializable
{
	private int dtidx;

	private static short compress(int dt)
	{
		int yy = (dt/10000) - 2000;
		int mm = (dt%10000)/100;
		int dd = (dt%100);
		return (short)(dd + (mm-1)*40+yy*500);
	}

	private static int decompress(short dt)
	{
		int res = dt;
		int yy = res/500;
		res -= yy * 500;
		int mm = 1 + (res/40);
		int dd = res % 40;
		return 20000000 + (yy*10000) + (mm * 100) + dd;
	}
	
	//datatype[1] - scd, datatype[31] - fcd
	private Serializable data;

	public Scd()  { }
	
	public Scd(int dt, FieldMeta m)
	{
		if (m.isfcd())
			dtidx = (dt/100)*100+1;
		else 
			dtidx = dt;
	}
	
	public void reset_dt(int dt) { dtidx = dt; } 
	public int fromdt() { return dtidx; } 
	
	public boolean has(int dt, FieldMeta m)
	{
		if (dt < dtidx) return false;
		return (m.isfcd())? (dt/100 == dtidx/100):true; //for FCD
	}

	//returns the latest values
	public Object spoto(FieldMeta m) { return geto(31, m); }
	public double spotd(FieldMeta m) { return getd(31, m); }
	
	public Object geto(int dt, FieldMeta m) {
		return m.getArrayo(data,(m.isfcd())? (dt%100-1):0);
	}
	
	public double getd(int dt, FieldMeta m) {
		return m.getArrayd(data,(m.isfcd())? (dt%100-1):0);
	}

	public void seto(int dt, FieldMeta m, Object o) 
	{
		int maxidx = (m.isfcd())? 31:1;
		if (data == null) data = m.toArray(maxidx);
		m.setArray(data,Math.min(dt%100,maxidx)-1,o);
	}
	
	public void setd(int dt, FieldMeta m, double d) {
		int maxidx = (m.isfcd())? 31:1;
		if (data == null) data = m.toArray(maxidx);
		m.setArray(data,Math.min(dt%100,maxidx)-1,d);
	}
	
	public int length(FieldMeta m)
	{
		return (m.isfcd())? 2+31*m.size():2+m.size();
	}
	
	public void serialize(ByteBuffer buff, FieldMeta m) 
	{
		if (m.isfcd()) {
			buff.putShort(compress(dtidx));
			m.fromArray(buff, data, 31);
		} else { 
			buff.putShort(compress(dtidx));
			m.fromArray(buff, data, 1);
		} 
	}
	
	public boolean deserialize(ByteBuffer buff, FieldMeta m) 
	{
		if (buff.remaining() < 4) 
			return false;
		if (m.isfcd()) {
			dtidx = decompress(buff.getShort());
			data = m.toArray(buff, 31);
		} else { 
			dtidx = decompress(buff.getShort());
			data = m.toArray(buff, 1);
		} 
		return data != null;	
	}
	
	public String toString()
	{
		return dtidx + "->";
	}
}
