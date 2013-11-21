package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    ArrayList<Page> m_pages;
    int maxNumPages;

	private LockManager m_LockManager;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
    	m_pages = new ArrayList<Page>();
    	maxNumPages = numPages;
    	m_LockManager = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
    	
    	boolean hasLock = getLockManager().getLock(perm, tid, pid);
    	while (!hasLock) {
    		try {
				Thread.sleep(5);
				hasLock = getLockManager().getLock(perm, tid, pid);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}    		
    	}
    	for (int i = 0; i < m_pages.size(); i++) {
    		if (m_pages.get(i).getId().equals(pid)) {
    			Page p = m_pages.remove(i);
    			m_pages.add(p);
    			return p;
    		}
    	}
    	Catalog catalog = Database.getCatalog();
    	Page newPage = catalog.getDbFile(pid.getTableId()).readPage(pid);
    	if (m_pages.size() >= maxNumPages)
    		evictPage();
    	m_pages.add(newPage);
    	return newPage;

    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
    	getLockManager().releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
    	for (Page p : m_pages) {
    		getLockManager().releaseLock(tid, p.getId());
    	}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId pid) {
    	return getLockManager().holdsLock(tid, pid);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
    	transactionComplete(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
    	HeapFile file = (HeapFile) Database.getCatalog().getDbFile(tableId);
    	ArrayList<Page> dirtiedPages = file.insertTuple(tid, t);
    	for (Page p:dirtiedPages) {
    		p.markDirty(true, tid);
    		m_pages.add(p);
    	}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
    	HeapFile file = (HeapFile) Database.getCatalog()
    			.getDbFile(t.getRecordId().getPageId().getTableId());
    	Page page = file.deleteTuple(tid, t);
    	page.markDirty(true, tid);    	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
    	for (Page page:m_pages) {
    		flushPage(page.getId());
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
    	HeapFile file = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());
    	for (Page page:m_pages) {
    		if (page.getId().equals(pid)) {
    			if (page.isDirty() != null)
    				file.writePage(page);
    		}
    	}
    	
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
    	Page evictedPage = m_pages.remove(0);
    	try {
			flushPage(evictedPage.getId());
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    private LockManager getLockManager() {
    	return m_LockManager;
    }
    
    private class LockManager {
    	HashMap<PageId, ArrayList<TransactionId>> sharedLocks = new HashMap<PageId, ArrayList<TransactionId>>();
    	HashMap<PageId, TransactionId> exclusiveLocks = new HashMap<PageId, TransactionId>();
    	
    	public boolean getLock(Permissions perm, TransactionId tid, PageId pid) {
    		if (Permissions.READ_ONLY.equals(perm)) {
    			if (exclusiveLocks.get(pid) != null)
    				return false;
    			if (sharedLocks.get(pid) == null) {
    				sharedLocks.put(pid, new ArrayList<TransactionId>());
    			}
    			sharedLocks.get(pid).add(tid);
    		} else if (Permissions.READ_WRITE.equals(perm)) {
    			if (exclusiveLocks.get(pid) != null || sharedLocks.get(pid) != null)
    				return false;
    			exclusiveLocks.put(pid, tid);
    		}
    		return true;
    	}
    	
		public boolean releaseLock(TransactionId tid, PageId pid) {
    		if (exclusiveLocks.get(pid) == null && sharedLocks.get(pid) == null)
    			return false;
    		sharedLocks.get(pid).remove(tid);
    		if (sharedLocks.get(pid).size() == 0)
    			sharedLocks.remove(pid);
    		exclusiveLocks.remove(pid);
    		return true;
    	}
    	
    	public boolean upgradeLock(TransactionId tid, PageId pid) {
    		return false;
    	}
    	
    	public boolean holdsLock(TransactionId tid, PageId pid) {
    		boolean s = sharedLocks.get(pid).contains(tid);
    		boolean e = exclusiveLocks.get(pid) != null;
    		return s || e;
    	}
    }

}
