package fgg.grpc;

import java.util.*;
import fgg.utils.*;
import fgg.data.*;
import java.util.stream.*;

//Entity cache on the client side
//Node, Edge, Attr represent entity types in a Graph system
//A Collection of instances of them together form a specific Graph  
public abstract class GraphItem
{
	public static class Node extends GraphItem 
	{
		private Node parent;

		private Set<NodeAttr> attrs = new HashSet<NodeAttr>();
		private Set<Integer> objkeys = new HashSet<Integer>();
		
		public Node(int id, String name) {
			super(id,name); 
		}
		
		public void addAttr(NodeAttr attr) { attrs.add(attr); }
		public void addObj(Integer key) { objkeys.add(key); }
		
		public void setParent(Node node) {
			parent = node;
		}			
	}; 
	
	public static class RootNode extends Node 
	{
		private Node leg;
		
		public RootNode(int id, String name) {
			super(id,name); 
		}

		public void setLeg(Node node) {
			leg = node;
		}			
	}
	
	public static class Edge extends GraphItem 
	{
		private Set<EdgeAttr> attrs = new HashSet<EdgeAttr>();
		private Set<Integer> linkkeys = new HashSet<Integer>();
		private List<Node> nodes = new ArrayList<Node>();
		
		public Edge(int id, String name) {
			super(id,name);
		}
		
		public void addAttr(EdgeAttr attr) 	{ attrs.add(attr); 	 }
		public void addLink(Integer key) 	{ linkkeys.add(key); }
		public void addNode(Node node) 		{ nodes.add(node);   }
		public int maxnodes()               { return nodes.size(); }

		public Node node(int index) {
            return (index < 0 || index > nodes.size())? null:nodes.get(index);
        }            

		public int index(Node node) { return index(node,0); } 
        //Applies if node repeats more than once in an edge
		public int index(Node node, int count) 
		{
            int cnt = 0;
			for (int i=0;i<nodes.size();i++) 
				if (nodes.get(i).ordinal() == node.ordinal())
                    if (cnt++ == count)
                        return i;
			return -1;
		}
	}; 
	
	public static class NodeAttr extends GraphItem 
	{
		private final Node owner;
		private final DataType dtype;
		private final boolean splitByLeg; 
		private final int altKeyNum; 
        private final int size;
		
		public NodeAttr(int id, String name, Node owner, DataType dtype, 
                        boolean splitByLeg, int key, int size) {
			super(id,name); this.owner = owner; this.dtype = dtype; 
            this.splitByLeg = splitByLeg;this.altKeyNum = key;
            this.size = size;
		}

		public int altKeyNum() { 
			return this.altKeyNum; 
		}
        
        public int size() {
            return this.size;
        }
		
		public DataType dtype() { 
			return this.dtype; 
		}
        
		public Node owner() { 
			return this.owner; 
		}
		
		public String toString() {  return super.toString() + ",Owner=(" + owner + "),DataType=" + dtype; } 
	}; 

	public static class EdgeAttr extends GraphItem 
	{
		private final Edge owner;
		private final DataType dtype;
		private final int size;
		
		public EdgeAttr(int id, String name, Edge owner, DataType dtype, int size) {
			super(id,name); this.owner = owner; this.dtype = dtype;this.size = size; 
		}

		public DataType dtype() { 
			return this.dtype; 
		}
		
		public Edge owner() { 
			return this.owner; 
		}

        public int size() {
            return this.size;
        }

		public String toString() {  return super.toString() + ",Owner=(" + owner + "),DataType=" + dtype; } 
	}; 
	
	private final int id;	
	private final String name;	

	public int ordinal() { return id;   }
	public String name() { return name; }
	
	private static List<GraphItem> graph_items = new ArrayList<GraphItem>();
	private static Map<Integer,GraphItem> graph_map = new HashMap<Integer,GraphItem>();
	
	private GraphItem(int id, String name)
	{
		this.id = id;
		this.name = name.toLowerCase();
		graph_items.add(this);
        graph_map.put(id,this);
	}

	public String toString() { 
		String cls = this.getClass().getName();
		cls = cls.substring(1+cls.indexOf("$"));
		return "type=" + cls + ",name=" + name + ",id=" + id; 
	} 

	//////////////  ACCESSOR FUNCTIONS //////////////////////////
	public static Node findNode(String name) { 
        return (Node)findByName(name); 
    }
    
	public static Edge findEdge(String name) { 
        return (Edge)findByName(name); 
    }

	public static Edge findDefaultEdge(Node[] nodes) 
    { 
		for (GraphItem item: graph_items) 
        {
            if (!(item instanceof Edge)) continue;
            Edge edge = (Edge)item;
            int nodecnt = 0;
            for (Node node:nodes) 
                if (edge.index(node) >= 0 && edge.name().indexOf(""+node.ordinal()) >= 0)
                    nodecnt++;
                else break;
            if (nodecnt == nodes.length) 
                return edge;
        }
        return null;
    }

	public static void print() {
        for (GraphItem item: graph_items)
            System.out.println(item);
    }
    
	public static GraphItem findByName(String name) 
    {
        name = name.toLowerCase();
		for (GraphItem item: graph_items)
			if (item.name().equals(name)) 
				return item;
		return null;
	}

	public static GraphItem findByPK(int id) 
    {
        return graph_map.get(id);
	}

	public static List<Edge> findEdges(String nodenames) {
        List<Edge> edges = new ArrayList<Edge>();
        List<Node> nodes = new ArrayList<Node>();

        if (nodenames == null || nodenames.length() == 0) 
        {
            for (GraphItem item: graph_items) 
                if (item instanceof Edge) 
                    edges.add((Edge)item);
            return edges;
        }
        nodenames = nodenames.toLowerCase();
        
		for (GraphItem item: graph_items) {
            if (!(item instanceof Node)) continue;
            if (nodenames.indexOf(item.name()) == -1) continue;
            nodes.add((Node)item);
        }

		for (GraphItem item: graph_items) {
            if (!(item instanceof Edge)) continue;
            if (nodes.size() == nodes.stream().mapToInt(n -> (((Edge)item).index(n) == -1)? 0:1).sum()) 
                edges.add((Edge)item);
        }
		return edges;
	}
	

	public static EdgeAttr findAttr(Edge edge, String name) {
        if (edge == null) return null;
		for (EdgeAttr at:findAttrs(edge,new ArrayList<EdgeAttr>()))
			if (at.name().equals(name)) 
				return at;
		return null;
	}
	
	public static NodeAttr findAttr(Node node, String name) {
        if (node == null) return null;
		for (NodeAttr at:findAttrs(node,new ArrayList<NodeAttr>()))
			if (at.name().equals(name)) 
				return at;
		return null;
	}

	public static List<EdgeAttr> findAttrs(Edge edge, List<EdgeAttr> result) {
		result.clear();
        if (edge != null) 
		graph_items.stream().
				filter(x -> x instanceof EdgeAttr).
				filter(x -> ((EdgeAttr)x).owner().ordinal() == edge.ordinal()).
				forEach(x -> result.add((EdgeAttr)x)); 
		return result;
	}
	
	public static List<NodeAttr> findAttrs(Node node, List<NodeAttr> result) {
		result.clear();
        if (node != null)
		graph_items.stream().
				filter(x -> x instanceof NodeAttr).
				filter(x -> ((NodeAttr)x).owner().ordinal() == node.ordinal()).
				forEach(x -> result.add((NodeAttr)x)); 
		return result;
	}
	
}

