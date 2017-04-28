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

	// Here you need to add the correct tuples to tuples_to_return
	// for this operation

	// It should be done with an efficient algorithm based on
	// sorting or hashing
        
        if (join_cond instanceof ANDConditional) {
	    for (int i = 0; i < ((ANDConditional) join_cond).my_conds.length; i++) {
		((ANDConditional)join_cond).my_conds[i].set_left_leaf_table(first_join_tab);
                ((ANDConditional)join_cond).my_conds[i].set_right_leaf_table(second_join_tab);
            }
	}
        else {
	    ((ComparisonConditional) join_cond).set_left_leaf_table(first_join_tab);
            ((ComparisonConditional) join_cond).set_right_leaf_table(second_join_tab);
        }
        
        ArrayList<Tuple> tuples1 = first_join_tab.evaluate();
        ArrayList<Tuple> tuples2 = second_join_tab.evaluate();
        ListIterator iterate_tuples1 = tuples1.listIterator(0);
        ListIterator iterate_tuples2 = tuples2.listIterator(0);
        
        

	profile_intermediate_tables(tuples_to_return);
	return tuples_to_return;

    }	

}