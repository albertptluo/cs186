package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId m_tid;
    private DbIterator m_child;
	private TupleDesc m_td;

	private boolean deleteTwice;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
    	m_tid = t;
    	m_child = child;
    	m_td = new TupleDesc(new Type[] {Type.INT_TYPE}, 
				new String[] {"Number of inserted tuples"});
    }

    public TupleDesc getTupleDesc() {
        return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	m_child.open();
    	deleteTwice = false;
    }

    public void close() {
    	super.close();
    	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	m_child.rewind();
    	deleteTwice = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (deleteTwice)
    		return null;
    	Tuple t;
    	int numTuples = 0;
    	while (m_child.hasNext()) {
			try {
				Database.getBufferPool().deleteTuple(m_tid, m_child.next());
				numTuples++;
			} catch (NoSuchElementException e) {
				//do nothing
				e.printStackTrace();
			}
    	}
    	t = new Tuple(getTupleDesc());
    	t.setField(0, new IntField(numTuples));
    	deleteTwice = true;
    	return t;
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
