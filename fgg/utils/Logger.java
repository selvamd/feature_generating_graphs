package fgg.utils;

import fgg.data.*;
import java.util.*;
import fgg.access.*;

public enum Logger
{
    SET_NODE_DATA, GET_NODE_DATA, ENUM_GROUP; 

    private static Set<Logger> active = new HashSet<Logger>();
    
    public static void enable(Logger l) { active.add(l); }
    public static void disable(Logger l) { active.remove(l); }
    
	public static void log(Logger l, String s) { 
        if (active.contains(l)) System.out.println(s);
    }
}
