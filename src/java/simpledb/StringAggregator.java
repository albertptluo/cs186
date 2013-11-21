package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int m_gbfield;
	private Type m_gbfieldtype;
	private int m_afield;
	private Op m_what;
	private HashMap<Field, IntField> aggregateHash;
	private TupleDesc td = null;
	private Field groupby;

	/**
	 * Aggregate constructor
	 * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
	 * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
	 * @param afield the 0-based index of the aggregate field in the tuple
	 * @param what aggregation operator to use -- only supports COUNT
	 * @throws IllegalArgumentException if what != COUNT
	 */

	public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		m_gbfield = gbfield;
		m_gbfieldtype = gbfieldtype;
		m_afield = afield;
		m_what = what;
		aggregateHash = new HashMap<Field, IntField>();
		if (what != Op.COUNT)
			throw new IllegalArgumentException("Operation must be COUNT");
		//aggregateHash.put(new IntField(Aggregator.NO_GROUPING), new IntField(0));
		
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the constructor
	 * @param tup the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		IntField count;
		if (tup.getField(m_afield) == null)
			return;
		if (m_gbfield == Aggregator.NO_GROUPING)
			groupby = new IntField(Aggregator.NO_GROUPING);
		else
			groupby = tup.getField(m_gbfield);
		if (aggregateHash.get(groupby) == null)
			count = new IntField(1);
		else
			count = new IntField(aggregateHash.get(groupby).getValue() + 1);
		aggregateHash.put(groupby, count); 
		if (td == null)
			td = makeTD(tup);
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 *
	 * @return a DbIterator whose tuples are the pair (groupVal,
	 *   aggregateVal) if using group, or a single (aggregateVal) if no
	 *   grouping. The aggregateVal is determined by the type of
	 *   aggregate specified in the constructor.
	 */
	public DbIterator iterator() {
		ArrayList<Tuple> arr = new ArrayList<Tuple>();
		Tuple tup;
		for (Field gb: aggregateHash.keySet()) {
			tup = new Tuple(td);
			if (((IntField)gb).getValue() == Aggregator.NO_GROUPING)
				tup.setField(0, aggregateHash.get(gb));
			else {
				tup.setField(0, gb);
				tup.setField(1, aggregateHash.get(gb));
			}
			arr.add(tup);
		}
		return new TupleIterator(td, arr);
	}
	
	private TupleDesc makeTD(Tuple tup) {
		Type[] types;
		String[] fields;
		String aggregateFieldName;
		String groupbyFieldName;
		aggregateFieldName = tup.getTupleDesc().getFieldName(m_afield);
		if (m_gbfield == Aggregator.NO_GROUPING) {
			types = new Type[] {Type.INT_TYPE};
			fields = new String[] {aggregateFieldName};
		}
		else {
			groupbyFieldName = tup.getTupleDesc().getFieldName(m_gbfield);
			types = new Type[] {tup.getTupleDesc().getFieldType(m_gbfield),Type.INT_TYPE};
			fields = new String[] {groupbyFieldName, aggregateFieldName};
		}
		return new TupleDesc(types, fields);
	}

}
