package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

	private static final long serialVersionUID = 1L;

	private int m_gbfield;
	private Type m_gbfieldtype;
	private int m_afield;
	private Op m_what;
	private HashMap<Field, Field> aggregateHash;
	private HashMap<Field, Integer> avgCounts;
	private HashMap<Field, Integer> avgSums;

	private Field groupby;
	private TupleDesc td;

	/**
	 * Aggregate constructor
	 * 
	 * @param gbfield
	 *            the 0-based index of the group-by field in the tuple, or
	 *            NO_GROUPING if there is no grouping
	 * @param gbfieldtype
	 *            the type of the group by field (e.g., Type.INT_TYPE), or null
	 *            if there is no grouping
	 * @param afield
	 *            the 0-based index of the aggregate field in the tuple
	 * @param what
	 *            the aggregation operator
	 */

	public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
		m_gbfield = gbfield;
		m_gbfieldtype = gbfieldtype;
		m_afield = afield;
		m_what = what;
		aggregateHash = new HashMap<Field, Field>();
		avgCounts = new HashMap<Field, Integer>();
		avgSums = new HashMap<Field, Integer>();
	}

	/**
	 * Merge a new tuple into the aggregate, grouping as indicated in the
	 * constructor
	 * 
	 * @param tup
	 *            the Tuple containing an aggregate field and a group-by field
	 */
	public void mergeTupleIntoGroup(Tuple tup) {
		Integer value = 0;
		IntField afield = (IntField) tup.getField(m_afield);
		if (m_gbfield == Aggregator.NO_GROUPING)
			groupby = new IntField(Aggregator.NO_GROUPING);
		else
			groupby = tup.getField(m_gbfield);
		
		switch (m_what) {
		case AVG:
			if (avgCounts.get(groupby) == null) {
				avgCounts.put(groupby, 1);
				avgSums.put(groupby, afield.getValue());
				value = afield.getValue();
			}
			else {
				int count = avgCounts.get(groupby) + 1;
				avgCounts.put(groupby, count);
				avgSums.put(groupby,  avgSums.get(groupby) + afield.getValue());
				value = avgSums.get(groupby)/count;
			}
			break;
		case MIN:
			if (aggregateHash.get(groupby) == null)
				value = afield.getValue();
			else
				value = Math.min(afield.getValue(), 
						((IntField)aggregateHash.get(groupby)).getValue());
			break;
		case MAX:
			if (aggregateHash.get(groupby) == null) 
				value = afield.getValue();
			else
				value = Math.max(afield.getValue(), 
						((IntField)aggregateHash.get(groupby)).getValue());
			break;
		case SUM:
			if (aggregateHash.get(groupby) != null) 
				value = ((IntField)aggregateHash.get(groupby)).getValue();
			value = value + afield.getValue();
			break;
		case COUNT:
			if (aggregateHash.get(groupby) == null)
				value = 1;
			else
				value = ((IntField)aggregateHash.get(groupby)).getValue() + 1;
			break;
		}
		
//		if (m_what.equals(Op.AVG)) {
//			if (avgCounts.get(groupby) == null) {
//				avgCounts.put(groupby, 1);
//				avgSums.put(groupby, afield.getValue());
//				value = afield.getValue();
//			}
//			else {
//				int count = avgCounts.get(groupby) + 1;
//				avgCounts.put(groupby, count);
//				avgSums.put(groupby,  avgSums.get(groupby) + afield.getValue());
//				value = avgSums.get(groupby)/count;
//			}
//		} else if (m_what.equals(Op.MAX)) {
//			if (aggregateHash.get(groupby) == null) 
//				value = afield.getValue();
//			else
//				value = Math.max(((IntField)aggregateHash.get(groupby)).getValue(), afield.getValue());
//		} else if (m_what.equals(Op.MIN)) {
//			if (aggregateHash.get(groupby) == null)
//				value = afield.getValue();
//			else
//				value = Math.min(afield.getValue(), 
//						((IntField)aggregateHash.get(groupby)).getValue());
//		} else if (m_what.equals(Op.SUM)) {
//			if (aggregateHash.get(groupby) != null) 
//				value = ((IntField)aggregateHash.get(groupby)).getValue();
//			value = value + afield.getValue();
//		} else { //m_what.equals(Op.COUNT)
//			if (aggregateHash.get(groupby) == null)
//				value = 1;
//			else
//				value = ((IntField)aggregateHash.get(groupby)).getValue() + 1;
//		}
		Field aValue = new IntField(value);
		aggregateHash.put(groupby, aValue); 
		if (td == null)
			td = makeTD(tup);
	}

	/**
	 * Create a DbIterator over group aggregate results.
	 * 
	 * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
	 *         if using group, or a single (aggregateVal) if no grouping. The
	 *         aggregateVal is determined by the type of aggregate specified in
	 *         the constructor.
	 */
	public DbIterator iterator() {
		ArrayList<Tuple> arr = new ArrayList<Tuple>();
		Tuple tup;
		for (Field gb: aggregateHash.keySet()) {
			tup = new Tuple(td);
			if (m_gbfield == Aggregator.NO_GROUPING)
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
