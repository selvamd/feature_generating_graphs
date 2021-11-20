package fgg.utils;

import java.util.*;

//A function represents the reduction operation applied on multiple values
//visited from a single path traversal
public class Reducers
{
	/////////////////////// Functions ////////////////////////////////////////////////
	//Default IFunction looks for first occurance of a value and returns as soon as it is found
	public static IFunction getFirst()
	{
		return new IFunction()
		{
			Object result = null;
			double dresult = 0;
			public void clear() { dresult = 0;result = null; };
			public boolean process(Object o, double d) { result = o;dresult = d;return (result == null); };
			public Object geto() { return result; };
			public double get() { return dresult; };
		};
	}

	public static IFunction getSum()
	{
		return new IFunction()
		{
			double dresult = 0;
			public void clear() { dresult = 0; };
			public boolean process(Object o, double d) { dresult += d; return true; };
			public Object geto() { return new Double(dresult); };
			public double get() { return dresult; };
		};
	}

	public static IFunction getStdDev()
	{
		return new IFunction()
		{
			double dsum = 0;
			double dsqsum = 0;
			int count = 0;
			public void clear() { dsqsum = 0;dsum = 0;count = 0; };
			public boolean process(Object o, double d) { count++;dsum += d;dsqsum += (d*d); return true; };
			public Object geto() { return new Double(Math.sqrt((dsqsum/count) - (dsum*dsum)/(count*count))); };
			public double get() { return (Math.sqrt((dsqsum/count) - (dsum*dsum)/(count*count))); };
		};
	}

	public static IFunction getAverage()
	{
		return new IFunction()
		{
			double dresult = 0;
			int count = 0;
			public void clear() { dresult = 0;count = 0; };
			public boolean process(Object o, double d) { count++;dresult += d; return true; };
			public Object geto() { return new Double(dresult/count); };
			public double get() { return (dresult/count); };
		};
	}

	public static IFunction getCount()
	{
		return new IFunction()
		{
			double dresult = 0;
			public void clear() { dresult = 0; };
			public boolean process(Object o, double d) { dresult += 1; return true; };
			public Object geto() { return new Long((long)dresult); };
			public double get() { return dresult; };
		};
	}

	public static IFunction getMin()
	{
		return new IFunction()
		{
			double dresult = 0;
			public void clear() { dresult = 999999999; };
			public boolean process(Object o, double d) { dresult = Math.min(dresult,d); return true; };
			public Object geto() { return new Long((long)dresult); };
			public double get() { return dresult; };
		};
	}

	public static IFunction getMax()
	{
		return new IFunction()
		{
			double dresult = 0;
			public void clear() { dresult = -999999999; };
			public boolean process(Object o, double d) { dresult = Math.max(dresult,d); return true; };
			public Object geto() { return new Long((long)dresult); };
			public double get() { return dresult; };
		};
	}

}
