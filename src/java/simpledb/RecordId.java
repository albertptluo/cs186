package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    private PageId m_pid;
    private int m_tupleno;
    
    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        //TODO
    	m_pid = pid;
    	m_tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int tupleno() {
        //TODO
        return m_tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        //TODO
    	return m_pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        //TODO
        return o!=null && 
        		o.getClass().equals(this.getClass()) &&
        		((RecordId) o).tupleno() == this.tupleno() &&
        		((RecordId) o).getPageId().equals(this.getPageId());
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	//TODO
    	return Integer.parseInt(m_pid.hashCode() + "" + m_tupleno);
    }

}
