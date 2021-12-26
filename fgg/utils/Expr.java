package fgg.utils;

import java.util.*;
import fgg.data.*;

public class Expr
{
	//An expression should bracket each operator individually as follows:
	//		Eg: ((($x / 2) > 10) & ((($y / 2) + 3) < 2))
	public enum Oper { LT, GT, EQ, LE, GE, NE, NOT, AND, OR, TYP, ADD, SUB, MUL, DIV, MOD, INT }

	//Comparison operators:  "<, >, =, <=, >=, !="
	//Special comparison for contain/childof: @
	//Logical operators:	!, &, |
	//Arithmetic operators: +, -, *, /, %, ~
	public static char[] OperSymbols = {'<', '>', '=', '@', '!', '&', '|', '+', '-', '*', '/', '%', '~' };

	//single select attributes
	private double val;
	private String sval;
	private String var;
	private boolean isstring;

	//For selector attributes, identify the
	//field and index to grab the values;
	private Oper oper;
	private Expr lhs;
	private Expr rhs;


	public static Expr getExpr(String expr)
	{
		try {
			return getExprPrivate(expr);
		}
		catch (Exception e)
		{
			System.out.println("Error parsing " + expr);
			e.printStackTrace();
		}
		return null;
	}

	private static Expr getExprPrivate(String expr)
	{
		expr = expr.trim();

		//Check if it is a single expression
		if ((expr.indexOf(")") == -1) && (expr.indexOf("(") == -1))
		{
			//Expr can only be a single value or single variable at this point
			Expr e = new Expr();
			if (expr.charAt(0) == '$') {
				e.var = expr.substring(1);
			}
			else if (expr.charAt(0) == '#') {
				e.var = expr.substring(1);
				e.isstring = true;
			}
			else 
			{
				e.isstring = expr.startsWith("'");
				e.sval = (e.isstring)? expr.substring(1,expr.length()-1):expr;
				e.val = parseDouble(e.sval);
			}

			return e;
		}
		else
		{
			//count and mincount tracks nesting level of each operator and
			//is used to find the operator that is least nested within the expression
			//idx tracks the char position of the least nested operator
			int mincount = 9999, idx = 0, count = 0;
			for (int i=0; i < expr.length();i++)
			{
				char c = expr.charAt(i);
				if (c == '(') count++;
				else if (c == ')') count--;
				else if (i > 0)
				{
					char prev = expr.charAt(i-1);
					// <= or >= or != or == is single operators, skip matching on 2nd char again
					if ((c == '=') && (prev == '!' || prev == '<' || prev == '>' || prev == '='))
							continue;

					for (char os: OperSymbols)
					{
						if (os != c) continue;
						if (mincount < count)
							continue;
						mincount = count;
						idx = i;
					}
				}
			}

			if (mincount == 0 || mincount == 1)
			{
				//Strip top level nesting if it exists
				if (mincount == 1 && expr.startsWith("(") && expr.endsWith(")"))
				{
					idx--;
					expr = expr.substring(1,expr.length()-1);
				}
				char c = expr.charAt(idx);

				//parse the lhs expression recursively
				Expr s1 = getExprPrivate(expr.substring(0,idx).trim());

				//parse the rhs expression recursively
				String rexpr = expr.substring(idx+1,expr.length());
				char c1 = rexpr.charAt(0);
				if (c1 == '=') rexpr = rexpr.substring(1);
				Expr s2 = getExprPrivate(rexpr.trim());

				//System.out.println("mincount="+mincount+",s1="+expr.substring(0,idx).trim()+",c="+c+",c1="+ c1+",s2="+rexpr.trim());
				if (s1 == null || s2 == null)
				{
					System.out.println("Filter expr is null == > " + expr);
					return null;
				}
				else return getExprPrivate(s1,c, c1, s2);
			}

			System.out.println("Filter expr is null == > " + expr);
			return null;
		}

	}

	private static double parseDouble(String s)
	{
		try {
			return Double.parseDouble(s);
		} catch (Exception e) {
		}
		return 0;
	}

	private static Expr getExprPrivate(Expr lhsSel, char oper, char oper1, Expr rhsSel)
	{
		Expr s = new Expr();
		s.lhs = lhsSel;
		s.rhs = rhsSel;
		if (s.lhs == null || s.rhs == null)
		{
			System.out.println("Invalid eval vars ");
			return null;
		}

		if (oper1 == '=')
		{
			if (oper == '<') s.oper = Oper.LE;
			if (oper == '>') s.oper = Oper.GE;
			if (oper == '!') s.oper = Oper.NE;
			if (oper == '=') s.oper = Oper.EQ;
		}
		else
		{
			if (oper == '<') s.oper = Oper.LT;
			if (oper == '>') s.oper = Oper.GT;
			if (oper == '=') s.oper = Oper.EQ;
			if (oper == '!') s.oper = Oper.NOT;
			if (oper == '&') s.oper = Oper.AND;
			if (oper == '|') s.oper = Oper.OR;
			if (oper == '@') s.oper = Oper.TYP;
			if (oper == '+') s.oper = Oper.ADD;
			if (oper == '-') s.oper = Oper.SUB;
			if (oper == '*') s.oper = Oper.MUL;
			if (oper == '/') s.oper = Oper.DIV;
			if (oper == '%') s.oper = Oper.MOD;
			if (oper == '~') s.oper = Oper.INT;
		}
		if (s.oper == null)
		{
			System.out.println("Invalid oper");
			return null;
		}
		return s;
	}

	//Gets all the variables in the expression
	public Set<String> getVars(Set<String> fields)
    {
        fields.clear();
        return getVarsPrivate(fields);
    }

	public Set<String> getVarsPrivate(Set<String> fields)
	{
		if (this.oper != null)
		{
			this.lhs.getVarsPrivate(fields);
			this.rhs.getVarsPrivate(fields);
		}
		if (this.var != null)
			fields.add(this.var);
		return fields;
	}

	public boolean filter(Record rec, int asof) {
		return false;
	}

	public boolean filter(Map<String,Object> values) {
		if (eval(values))
			if (this.val == 1)
				return true;
		return false;
	}

	public boolean validate(Set<String> fields)
	{
		if (this.oper != null)
		{
			if (!this.lhs.validate(fields)) return false;
			if (!this.rhs.validate(fields)) return false;
			return true;
		}
		if (this.var != null) return fields.contains(this.var);
		return true;
	}

	//Evaluates the expression given all the variables
	public boolean eval(Map<String,Object> values)
	{
		if (this.var != null)
		{
			Object o = values.get(this.var);
			if (o == null) return false;
			this.sval = o.toString();
			if (!(o instanceof String))
				this.val = parseDouble(this.sval);
			//System.out.println(this.var+","+this.sval+","+this.val+","+o.toString());
			return true;
		}
		if (this.oper != null)
		{
			if (!this.lhs.eval(values)) return false;
			if (!this.rhs.eval(values)) return false;
			if (this.oper == Oper.ADD) this.val = this.lhs.val + this.rhs.val;
			if (this.oper == Oper.SUB) this.val = this.lhs.val - this.rhs.val;
			if (this.oper == Oper.MUL) this.val = this.lhs.val * this.rhs.val;
			if (this.oper == Oper.DIV) this.val = this.lhs.val / this.rhs.val;
			if (this.oper == Oper.MOD) this.val = ((int)this.lhs.val) % ((int)this.rhs.val);
			if (this.oper == Oper.INT) this.val = ((int)this.rhs.val);
			if (this.oper == Oper.LT) this.val 	= (this.lhs.val < this.rhs.val)? 1:0;
			if (this.oper == Oper.GT) this.val 	= (this.lhs.val > this.rhs.val)? 1:0;
			if (this.oper == Oper.EQ) this.val 	= (this.lhs.val == this.rhs.val)? 1:0;
			if (this.oper == Oper.LE) this.val 	= (this.lhs.val <= this.rhs.val)? 1:0;
			if (this.oper == Oper.GE) this.val 	= (this.lhs.val >= this.rhs.val)? 1:0;
			if (this.oper == Oper.NE) this.val 	= (this.lhs.val != this.rhs.val)? 1:0;
			if (this.oper == Oper.NOT) this.val = (this.rhs.val == 0)? 1:0;
			if (this.oper == Oper.AND) this.val = (this.lhs.val != 0 && this.rhs.val != 0)? 1:0;
			if (this.oper == Oper.OR) this.val  = (this.lhs.val != 0 || this.rhs.val != 0)? 1:0;
			if (this.oper == Oper.TYP)
			{
				System.out.println(Oper.TYP + "NOT SUPPORTED");
				this.val = 0;
			}
			//	this.val = (Group.isinstanceof(Group.groupkey(this.lhs.var,this.lhs.sval),
			//					this.lhs.var +"."+ this.rhs.sval))? 1:0;
			if (this.lhs.isstring || this.rhs.isstring)
			{
				boolean result = false;
				if (this.oper == Oper.ADD) this.sval =  this.lhs.sval + this.rhs.sval;
				int res = this.lhs.sval.compareTo(this.rhs.sval);
				if (res == 0) 	result = (this.oper == Oper.EQ) || (this.oper == Oper.GE) || (this.oper == Oper.LE);
				if (res < 0) 	result = (this.oper == Oper.LT) || (this.oper == Oper.LE) || (this.oper == Oper.NE);
				if (res > 0) 	result = (this.oper == Oper.GT) || (this.oper == Oper.GE) || (this.oper == Oper.NE);
				//System.out.println(this.lhs.sval + ":" + this.rhs.sval+ ":" + res + ":" + result);
				this.val = (result)? 1:0;
			}
		}
		return true;
	}

	public double getValue()
	{
		return this.val;
	}

	public static void main(String[] args)
	{
		//Expr expr = getExpr("((($x / 2) >= 10) | ((($y / 2) + 3) <= 4.5))");
		//Expr expr = getExpr("(($x < 3) & ($y > 3)) | ($z < 3)");
		//Expr expr = getExpr("$x & ($y | $z)");
		Expr expr = getExpr("(#x < #y)");

		Map<String,Object> values = new HashMap<String,Object>();
		values.put("x","new Integer(12)");
		values.put("y","new Long(2)");
		if (expr.eval(values))
			System.out.println(expr.val);
		values.put("y",new Long(3));
		if (expr.eval(values))
			System.out.println(expr.val);
		values.put("y",new Long(4));
		if (expr.eval(values))
			System.out.println(expr.val);
		Set<String> s = new HashSet<String>();
		expr.getVars(s);
		System.out.println(s);

	}


}
