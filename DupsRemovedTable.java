import java.util.*;

/**
 * DupsRemovedTable - this class implements the duplicate removal operation of a
 * relational DB
 *
 */

public class DupsRemovedTable extends Table {

    Table tab_dups_removed_from;

    /**
     * @param t - the table from which duplicates are to be removed
     *
     */
    public DupsRemovedTable(Table t) {

	super("Removing duplicates from " + t.toString());
	tab_dups_removed_from = t;

        attr_names = t.attrib_names();
	attr_types = t.attrib_types();
    }

    public Table [] my_children () {
	return new Table [] { tab_dups_removed_from };
    }

    public Table optimize() {
	// Right now no optimization is done -- you'll need to improve this
	return this;
    }	

    public ArrayList<Tuple> evaluate() {
	ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();
        
        ArrayList<Tuple> tuples = tab_dups_removed_from.evaluate();
        
        //Sort tuples based on first attribute
        tuples.sort(new TupleComparator());
        
        
        //Iterate through sorted tuples to check if tuple is a duplicate
        //If there are duplicates, the last one will be added to the table
        ListIterator iterate_tuples1 = tuples.listIterator();
        while (iterate_tuples1.hasNext())
        {
            Tuple x = (Tuple) iterate_tuples1.next();
            boolean x_is_duplicate = false;
            String first_attr = attr_names[0];
            ColumnValue first_attr_val_x = x.get_val(first_attr);
            ListIterator iterate_tuples2 = tuples.listIterator(tuples.indexOf(x) + 1);
            while (iterate_tuples2.hasNext())
            {
                Tuple y = (Tuple) iterate_tuples2.next();
                ColumnValue first_attr_val_y = y.get_val(first_attr);
                //Stop checking tuples if the first attribute is not equal
                if (!first_attr_val_x.equals(first_attr_val_y)) {
                    break;
                }
                //If all attributes are equal in x and y, they are duplicates
                boolean x_equals_y = true;
                for (int i = 1; i < attr_names.length; i++) {
                    String attr = attr_names[i];
                    ColumnValue attr_val_x = x.get_val(attr);
                    ColumnValue attr_val_y = y.get_val(attr);
                    //If any attributes are not equal, x and y are not duplicates
                    if (!attr_val_x.equals(attr_val_y)) {
                        x_equals_y = false;
                        break;
                    }
                }
                if (x_equals_y) {
                    x_is_duplicate = true;
                    break;
                }
            }
            //If x is not a duplicate, add it to tuples_to_return
            if (!x_is_duplicate) {
                tuples_to_return.add(x);
            }
         }
        

	profile_intermediate_tables(tuples_to_return);
	return tuples_to_return;

    }
    
    public class TupleComparator implements Comparator<Tuple> {
        @Override
        public int compare(Tuple t1, Tuple t2) {
            if (attr_names.length < 1) {
                return 0;
            }
            String attr = attr_names[0];
            ColumnValue val1 = t1.get_val(attr);
            ColumnValue val2 = t2.get_val(attr);
            return val1.compareTo(val2);
        }
    }

}