package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    ArrayList<TDItem> m_TDItems = new ArrayList<TDItem>();
    
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return m_TDItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	for (int i = 0; i < typeAr.length; i++) {
    		String field = null;
    		if (i < fieldAr.length)
    			field = fieldAr[i];
    		TDItem item = new TDItem(typeAr[i], field);
    		m_TDItems.add(item);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	for (int i = 0; i < typeAr.length; i++) {
    		TDItem item = new TDItem(typeAr[i], null);
    		m_TDItems.add(item);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return m_TDItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        return m_TDItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        return m_TDItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	for (int i = 0; i < numFields(); i++) {
    		String fieldName = m_TDItems.get(i).fieldName;
    		if (null == name || null == fieldName) {
    			if (name == fieldName)
    				return i;
    		}
    		else {
    			if (fieldName.equals(name))
    				return i;
    		}
    	}
    	throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
    	for (TDItem tditem:m_TDItems) {
    		size += tditem.fieldType.getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	Type[] types = new Type[td1.numFields() + td2.numFields()];
    	String[] names = new String[td1.numFields() + td2.numFields()];
    	Iterator<TDItem> iterator1 = td1.iterator();
    	Iterator<TDItem> iterator2 = td2.iterator();
    	int index = 0;
    	while (iterator1.hasNext()) {
    		TDItem item = iterator1.next();
    		types[index] = item.fieldType;
    		names[index] = item.fieldName;
    		index++;
    	}
    	while (iterator2.hasNext()) {
    		TDItem item = iterator2.next();
    		types[index] = item.fieldType;
    		names[index] = item.fieldName;
    		index++;
    	}
    	return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	if (o != null && o.getClass().equals(this.getClass()) &&
    			this.getSize() == ((TupleDesc) o).getSize()) {
    		for (int i = 0; i < m_TDItems.size(); i++) {
    			if (m_TDItems.get(i).fieldType != ((TupleDesc) o).m_TDItems.get(i).fieldType)
    				return false;
    		}
    		return true;
    	}
    	return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
    	String result = "";
    	for (int i = 0; i < m_TDItems.size(); i++) {
    		result = result + m_TDItems.get(i).toString() + ", ";
    	}
        return result;
    }
}
