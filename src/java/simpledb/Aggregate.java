package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    
    private DbIterator m_child;
    private int m_afield;
    private int m_gfield;
    private Aggregator.Op m_aop;
    private Aggregator aggregator;
    private DbIterator results;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		m_child = child;
		m_afield = afield;
		m_gfield = gfield;
		m_aop = aop;
		Type gbfieldtype = (gfield < 0 ? null : m_child.getTupleDesc().getFieldType(gfield));
		if (m_child.getTupleDesc().getFieldType(afield).equals(Type.INT_TYPE))
			aggregator = new IntegerAggregator(gfield, gbfieldtype, afield, aop);
		else
			aggregator = new StringAggregator(gfield, gbfieldtype, afield, aop);
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return m_gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	return (groupField() == Aggregator.NO_GROUPING ? null : m_child.getTupleDesc().getFieldName(groupField()));
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return m_afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return m_child.getTupleDesc().getFieldName(aggregateField());
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return m_aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
    	super.open();
    	m_child.open();
    	while (m_child.hasNext()) {
    		aggregator.mergeTupleIntoGroup(m_child.next());
    	}
    	results = aggregator.iterator();
    	results.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (results == null) {
    		throw new DbException("fetchNext() called before open()");
    	}
    	if (results.hasNext())
    		return results.next();
    	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
		results.rewind();
		m_child.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	TupleDesc td = null;
    	Type[] types;
    	String[] fields;
    	String aggregateName = m_aop.toString() 
    							+ "(" 
    							+ m_child.getTupleDesc().getFieldName(aggregateField()) 
    							+ ")";
    	if (groupField() == Aggregator.NO_GROUPING) {
    		//only one field = aggregate column
    		types = new Type[] {m_child.getTupleDesc().getFieldType(aggregateField())};
    		fields = new String[] {aggregateName};
    		td = new TupleDesc(types, fields);
    	}
    	else {
    		types = new Type[] {m_child.getTupleDesc().getFieldType(groupField()),
    				m_child.getTupleDesc().getFieldType(aggregateField())};
    		fields = new String[] {groupFieldName(), aggregateName};
    		td = new TupleDesc(types, fields);
    	}
    	return td;
    }

    public void close() {
    	super.close();
    	results.close();
    	m_child.close();
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] {m_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	m_child = children[0];
    }
    
}
