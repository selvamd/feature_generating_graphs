package fgg.utils;

public interface IFunction
{
	//If there is no need to go further, Functions can stop further
	//path evaluations from happening by returning false.
	public boolean process(Object o, double d);
	public Object geto();
	public double get();
	public void clear();

	public static IFunction getFunction(String str)
	{
		if (str.equalsIgnoreCase("SUM"))
			return Reducers.getSum();
		else if (str.equalsIgnoreCase("COUNT"))
			return Reducers.getCount();
		else if (str.equalsIgnoreCase("average") ||
			str.equalsIgnoreCase("avg"))
			return Reducers.getAverage();
		else if (str.equalsIgnoreCase("min"))
			return Reducers.getMin();
		else if (str.equalsIgnoreCase("max"))
			return Reducers.getMax();
		else if (str.equalsIgnoreCase("first"))
			return Reducers.getFirst();
		else if (str.equalsIgnoreCase("stddev"))
			return Reducers.getStdDev();
		return null;
	}

};
