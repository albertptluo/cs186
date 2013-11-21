package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    
    private Predicate m_predicate;
    private DbIterator m_child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        m_predicate = p;
        m_child = child;
    }

    public Predicate getPredicate() {
        return m_predicate;
    }

    public TupleDesc getTupleDesc() {
        return m_child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        m_child.open();
        super.open();
    }

    public void close() {
        m_child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	m_child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while (m_child.hasNext()) {
        	Tuple next = m_child.next();
        	if (m_predicate.filter(next))
        		return next;
        }
        return null;
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
