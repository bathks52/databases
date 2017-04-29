import java.util.*;

/**
 * JoinTable - this class implements the join operation of a
 * relational DB
 *
 */

public class JoinTable extends Table {

    Table first_join_tab;
    Table second_join_tab;
    Conditional join_cond;

    /**
     * @param t1 - One of the tables for the join
     * @param t2 - The other table for the join.  You are guaranteed
     * that the tables do not share any common attribute names.
     * @param c - the conditional used to make the join
     *
     */
    public JoinTable(Table t1, Table t2, Conditional c) {

	super("Joining " + t1.toString() + " " + t2.toString() + " on condiition " + c.toString());
	first_join_tab = t1;
	second_join_tab = t2;
        join_cond = c;
        
        attr_names = concat(t1.attr_names, t2.attr_names);
        attr_types = concat(t1.attr_types, t2.attr_types);
    }
    
    private String[] concat(String[] a, String[] b) {
         int aLen = a.length;
         int bLen = b.length;
         String[] c= new String[aLen+bLen];
         System.arraycopy(a, 0, c, 0, aLen);
         System.arraycopy(b, 0, c, aLen, bLen);
         return c;
    }

    public Table [] my_children () {
	return new Table [] { first_join_tab, second_join_tab };
    }

    public Table optimize() {
	// Right now no optimization is done -- you'll need to improve this
	return this;
    }	

    public ArrayList<Tuple> evaluate() {
	ArrayList<Tuple> tuples_to_return = new ArrayList<Tuple>();
        
//        if (join_cond instanceof ANDConditional) {
//	    for (int i = 0; i < ((ANDConditional) join_cond).my_conds.length; i++) {
//		((ANDConditional)join_cond).my_conds[i].set_left_leaf_table(first_join_tab);
//                ((ANDConditional)join_cond).my_conds[i].set_right_leaf_table(second_join_tab);
//            }
//	}
//        else {
//	    ((ComparisonConditional) join_cond).set_left_leaf_table(first_join_tab);
//            ((ComparisonConditional) join_cond).set_right_leaf_table(second_join_tab);
//        }
        
        ArrayList<Tuple> tuples1 = first_join_tab.evaluate();
        ArrayList<Tuple> tuples2 = second_join_tab.evaluate();
        
        if (join_cond instanceof ANDConditional) {
            
        }
        else {
            // Sort lists on join key
            String sortAttrLeft = ((ComparisonConditional)join_cond).left.attrib_name;
            String sortAttrRight = ((ComparisonConditional)join_cond).right.attrib_name;
            
            // Check which table each side of the condition uses
            if (((ComparisonConditional)join_cond).left.my_table.equals(first_join_tab)) {
                tuples1.sort(new TupleComparator(first_join_tab.attr_names, sortAttrLeft));
                if (((ComparisonConditional)join_cond).right.my_table.equals(second_join_tab)) {
                    tuples2.sort(new TupleComparator(second_join_tab.attr_names, sortAttrRight));
                }
            }
            else if (((ComparisonConditional)join_cond).left.my_table.equals(second_join_tab)){
                tuples2.sort(new TupleComparator(second_join_tab.attr_names, sortAttrLeft));
                if (((ComparisonConditional)join_cond).right.my_table.equals(first_join_tab)) {
                    tuples1.sort(new TupleComparator(first_join_tab.attr_names, sortAttrRight));
                }
            }
            
            ListIterator iterate_tuples1 = tuples1.listIterator(0);
            ListIterator iterate_tuples2 = tuples2.listIterator(0);
            
            while (iterate_tuples1.hasNext()) {
                Tuple t1 = (Tuple)iterate_tuples1.next();
                while (iterate_tuples2.hasNext()) {
                    Tuple t2 = (Tuple)iterate_tuples2.next();
                    //Check for equality somehow
                }
            }
        }
        
        //ListIterator iterate_tuples1 = tuples1.listIterator(0);
        //ListIterator iterate_tuples2 = tuples2.listIterator(0);
        
        // Just testing, ignore
        tuples_to_return.addAll(tuples1);
        tuples_to_return.addAll(tuples2);
        
	profile_intermediate_tables(tuples_to_return);
	return tuples_to_return;

    }
    
    public class TupleComparator implements Comparator<Tuple> {
       
        public String sortBy;
        public String[] attr_names;
        
        public TupleComparator(String[] attr_names, String attrib_name) {
           sortBy = attrib_name;
           this.attr_names = attr_names;
        }
       
        @Override
        public int compare(Tuple t1, Tuple t2) {
            if (attr_names.length < 1) {
                return 0;
            }
            //String attr = attr_names[0];
            ColumnValue val1 = t1.get_val(sortBy);
            ColumnValue val2 = t2.get_val(sortBy);
            return val1.compareTo(val2);
        }
    }

}