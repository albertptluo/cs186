package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId m_tid;
    private DbIterator m_child;
    private int m_tableid;
    private TupleDesc m_td;
    private boolean insertTwice;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
    	m_tid = t;
    	m_child = child;
    	m_tableid = tableid;
    	m_td = new TupleDesc(new Type[] {Type.INT_TYPE}, 
    						new String[] {"Number of inserted tuples"});
    	insertTwice = false;
    }

    public TupleDesc getTupleDesc() {
    	return m_td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
        m_child.open();
    }

    public void close() {
    	super.close();
    	m_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	m_child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instance of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (insertTwice)
    		return null;
    	Tuple t;
    	int numTuples = 0;
    	while (m_child.hasNext()) {
			try {
				Database.getBufferPool().insertTuple(m_tid, m_tableid, m_child.next());
				numTuples++;
			} catch (NoSuchElementException e) {
				//do nothing
				e.printStackTrace();
			} catch (IOException e) {
				//do nothing
				e.printStackTrace();
			}
    	}
    	t = new Tuple(getTupleDesc());
    	t.setField(0, new IntField(numTuples));
    	insertTwice = true;
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
