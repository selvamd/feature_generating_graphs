package fgg.grpc;
import java.util.Set;
import java.util.List;
import java.util.SortedSet;


//rootcursor = Cursor on objects (created if invoked from ObjectStore)
//linkedcursor = Cursor recursively created if invoked from another cursor
//               Currently linked-cursors are designed for 2 nodes only.
public interface ObjectCursor
{
    //Gets linkedcursor using default-edge
    public ObjectCursor link(String nodeName, int asofdt);

    //Gets linkedcursor using named-edge
    //Edges with self-linking nodes always have to be named and implicitly link from 1st -> 2nd
    public ObjectCursor linkByName(String edgeName, int asofdt);

    public Set<Integer> keys(Set<Integer> set);

    //Moves the cursor to the next object
    public boolean next();

    public int size();

    //Set list of attrs to load
    public boolean selectAttrs(String attrlist);

    //Set list of edge attrs to load (for linkedcursors only)
    public boolean selectLinkAttrs(String attrlist);

    public int getObjectPk(); //invoked on rootcursor only
    public int getLinkPk();   //invoked on linkedcursor only

    //Retrieves attr value for the current object
    public String get(String attr, int asofdt);

    //List of all attrs available
    public List<String> attrs();


    //Retrieves all the change dates for select fields
    public SortedSet<Integer> getScdDates(SortedSet<Integer> out);

    //Updates attr value for the current object locally
    public void set(String attr, int asofdt, String value);

    //Publish the values to the server
    public void publish();
}
