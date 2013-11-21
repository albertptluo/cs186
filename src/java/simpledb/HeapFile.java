package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	File m_file;
	TupleDesc m_td;

	/**
	 * Constructs a heap file backed by the specified file.
	 * 
	 * @param f
	 *            the file that stores the on-disk backing store for this heap
	 *            file.
	 */
	public HeapFile(File f, TupleDesc td) {
		m_file = f;
		m_td = td;
	}

	/**
	 * Returns the File backing this HeapFile on disk.
	 * 
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		return m_file;
	}

	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableid somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 * 
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		return m_file.getAbsoluteFile().hashCode();
	}

	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 * 
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		return m_td;
	}

	// see DbFile.java for javadocs
	/*
	 * To read a page from disk, you will first need to calculate the 
	 * correct offset in the file. Hint: you will need random access 
	 * to the file in order to read and write pages at arbitrary offsets. 
	 * You should not call BufferPool methods when reading a page from disk. 
	 * 
	 * @see simpledb.DbFile#readPage(simpledb.PageId)
	 */
	public Page readPage(PageId pid) {
		RandomAccessFile file = null;
		byte[] bytes = new byte[BufferPool.PAGE_SIZE];
		HeapPage page = null;
		try {
			file = new RandomAccessFile(getFile(), "r");
			file.seek(pid.pageNumber()*BufferPool.PAGE_SIZE);
			file.read(bytes);
			file.close();
			page = new HeapPage((HeapPageId)pid, bytes);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (IOException e) {
			//do nothing
			e.printStackTrace();
			return null;
		}
		return page;
	}

	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		RandomAccessFile file = null;
		byte[] bytes = page.getPageData();
		try {
			file = new RandomAccessFile(getFile(), "rw");
			file.seek(page.getId().pageNumber()*BufferPool.PAGE_SIZE);
			file.write(bytes);
			file.close();
		} catch (FileNotFoundException e) {
			//do nothing
			e.printStackTrace();
		}
	}

	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		return (int) Math.ceil(m_file.length()/BufferPool.PAGE_SIZE);
	}

	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		BufferPool bufferpool = Database.getBufferPool();
		if (t == null)
			throw new DbException("Tuple is null!");
		ArrayList<Page> pages = new ArrayList<Page>();
		for (int i = 0; i < numPages(); i++) {
			PageId pid = new HeapPageId(getId(), i);
			HeapPage page = (HeapPage) bufferpool.getPage(tid, pid, Permissions.READ_WRITE);
			if (page.getNumEmptySlots() != 0) {
				page.insertTuple(t);
				pages.add(page);
				break;
			}
		}
		if (pages.isEmpty()) {
			PageId pid = new HeapPageId(getId(), numPages());
			
			try {
				byte[] bytes = HeapPage.createEmptyPageData();
				RandomAccessFile file = new RandomAccessFile(getFile(), "rw");
				file.seek(pid.pageNumber()*BufferPool.PAGE_SIZE);
				file.write(bytes);
				file.close();
			} catch (FileNotFoundException e) {
				//do nothing
				e.printStackTrace();
			}
			HeapPage page = (HeapPage) bufferpool.getPage(tid, pid, Permissions.READ_WRITE);
			page.insertTuple(t);
			pages.add(page);			
		}
		return pages;
	}

	// see DbFile.java for javadocs
	public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
	TransactionAbortedException {
		BufferPool bufferpool = Database.getBufferPool();
		PageId pid = t.getRecordId().getPageId();
		HeapPage page = (HeapPage) bufferpool.getPage(tid, pid, Permissions.READ_WRITE);
		page.deleteTuple(t);
		return page;
	}

	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		return new HeapFileIterator(tid);
	}

	private class HeapFileIterator implements DbFileIterator {
		private TransactionId m_tid;
		private int pageIndex;
		private Iterator<Tuple> iterator;
		private boolean isOpen;

		public HeapFileIterator(TransactionId tid) {
			m_tid = tid;
			pageIndex = 0;
		}

		@Override
		public void open() throws DbException, TransactionAbortedException {
			isOpen = true;
			HeapPageId pid = new HeapPageId(getId(), pageIndex);
			HeapPage page = 
					(HeapPage) Database.getBufferPool().
					getPage(m_tid, pid, Permissions.READ_ONLY);
			iterator = page.iterator();
		}

		@Override
		public boolean hasNext() throws DbException,
		TransactionAbortedException {
			if (isOpen) {
				
				if (iterator == null) 
					return false;
				if (iterator.hasNext())
					return true;
				//pageIndex++;
				while(pageIndex < numPages()-1) {
					pageIndex++;
					HeapPageId pid = new HeapPageId(getId(), pageIndex);
					HeapPage page = 
							(HeapPage) Database.getBufferPool().
							getPage(m_tid, pid, Permissions.READ_ONLY);
					iterator = page.iterator();
					if (iterator.hasNext()) {
						return true;
					}
				}
				return false;
			}
//							
//				if (!iterator.hasNext()) {
//					pageIndex++;
//					if (pageIndex < numPages()) {
//						HeapPageId pid = new HeapPageId(getId(), pageIndex);
//						HeapPage page = 
//								(HeapPage) Database.getBufferPool().
//								getPage(m_tid, pid, Permissions.READ_ONLY);
//						iterator = page.iterator();
//						return iterator.hasNext();
//					}
//				}
//				return iterator.hasNext();
//			}
			return false;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException,
									NoSuchElementException {
			if (isOpen) {
				if (hasNext()) {
					return iterator.next();
				}
			}
			throw new NoSuchElementException();
		}
		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}
		
		@Override
		public void close() {
			pageIndex = 0;
			isOpen = false;
			iterator = null;
		}    	
	}

}

