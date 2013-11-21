package simpledb;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	private HashMap<Integer, Integer> histogram;
	private int m_min;
	private int m_max;
	private int m_buckets;
	private int m_bucketSize;
	private int m_totalElements;

	/**
	 * Create a new IntHistogram.
	 * 
	 * This IntHistogram should maintain a histogram of integer values that it receives.
	 * It should split the histogram into "buckets" buckets.
	 * 
	 * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
	 * 
	 * Your implementation should use space and have execution time that are both
	 * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
	 * simply store every value that you see in a sorted list.
	 * 
	 * @param buckets The number of buckets to split the input value into.
	 * @param min The minimum integer value that will ever be passed to this class for histogramming
	 * @param max The maximum integer value that will ever be passed to this class for histogramming
	 */
	public IntHistogram(int buckets, int min, int max) {
		histogram = new HashMap<Integer, Integer>();
		m_buckets = buckets;
		m_min = min;
		m_max = max;
		m_totalElements = 0;
		m_bucketSize = (m_max - m_min)/m_buckets + 1;
		for (int i = 1; i <= buckets; i++) { 
			histogram.put(i, 0);
		}
	}

	/**
	 * Add a value to the set of values that you are keeping a histogram of.
	 * @param v Value to add to the histogram
	 */
	public void addValue(int v) {
		int bucketNumber = (v - m_min)/m_bucketSize + 1;
		histogram.put(bucketNumber, histogram.get(bucketNumber) + 1);
		m_totalElements += 1;
		/*		5 numbuckets, min = 1, max = 5 
		 * 		(1) (2) (3) (4) (5)
		 * 		bucketsize = (max - min) / numbuckets = (5 - 1)/5 + 1= 1 
		 * 		
		 * 		5 numbuckets, min = -2, max = 7
		 * 		(-2, -1) (0, 1) (2, 3) (4, 5) (6, 7)
		 * 		bucketsize = (max - min) / numbuckets = (7 - -2)/5 + 1= 2 
		 * 			
		 * 		5 numbuckets, min = -7, max = 7
		 * 		(-7, -6, -5) (-4, -3, -2) (-1, 0, 1) (2, 3, 4) (5, 6, 7)
		 * 		bucketsize = (max - min) / numbuckets = (7 - -7)/5 + 1 = 14/5 + 1 = 3
		 * 		v = -7 should be in the 1st bucket
		 * 		(-7 - -7)/3 + 1
		 * 		v = 0 should be in the 3rd bucket
		 * 		(0 - -7)/3 + 1 
		 */
	}

	/**
	 * Estimate the selectivity of a particular predicate and operand on this table.
	 * 
	 * For example, if "op" is "GREATER_THAN" and "v" is 5, 
	 * return your estimate of the fraction of elements that are greater than 5.
	 * 
	 * @param op Operator
	 * @param v Value
	 * @return Predicted selectivity of this particular operator and value
	 */
	public double estimateSelectivity(Predicate.Op op, int v) {
		if (v < m_min) {
			if (op.equals(Predicate.Op.GREATER_THAN) || 
				op.equals(Predicate.Op.GREATER_THAN_OR_EQ) || 
				op.equals(Predicate.Op.NOT_EQUALS))
				return 1;
			else
				return 0;
		}
		if (v > m_max) {
			if (op.equals(Predicate.Op.LESS_THAN) || 
				op.equals(Predicate.Op.LESS_THAN_OR_EQ) || 
				op.equals(Predicate.Op.NOT_EQUALS))
				return 1;
			else
				return 0;
		}
		int bucketNumber = (v - m_min)/m_bucketSize + 1;
		int numElements = 0;
		
		switch(op) {
		case GREATER_THAN:
			for (int i = bucketNumber + 1; i <= m_buckets; i++) {
				numElements += histogram.get(i);
			}
			numElements += (bucketNumber * m_bucketSize + m_min - 1 - v)*histogram.get(bucketNumber)/m_bucketSize;
			break;
		case GREATER_THAN_OR_EQ:
			for (int i = bucketNumber + 1; i <= m_buckets; i++) {
				numElements += histogram.get(i);
			}
			numElements += (bucketNumber * m_bucketSize + m_min - 1 - v)*histogram.get(bucketNumber)/m_bucketSize;
			numElements += histogram.get(bucketNumber)/m_bucketSize;
			break;
		case LESS_THAN:
			for (int i = 1; i < bucketNumber; i++) {
				numElements += histogram.get(i);
			}
			//numElements += (v - (righthand of previous bucket)) * num things in bucket / bucketsize
			numElements += (v - ((bucketNumber - 1) * m_bucketSize + m_min))*histogram.get(bucketNumber)/m_bucketSize;
			
//			for (int i = bucketNumber + 1; i <= m_buckets; i++) {
//				numElements += histogram.get(i);
//			}
//			numElements += (bucketNumber * m_bucketSize + m_min - v)*histogram.get(bucketNumber)/m_bucketSize;
//			numElements += histogram.get(bucketNumber)/m_bucketSize;
//			numElements = m_totalElements - numElements;
			break;
		case LESS_THAN_OR_EQ:
			for (int i = 1; i < bucketNumber; i++) {
				numElements += histogram.get(i);
			}
			numElements += (v - ((bucketNumber - 1) * m_bucketSize + m_min))*histogram.get(bucketNumber)/m_bucketSize;
			//v v-1 lefthandside
			numElements += histogram.get(bucketNumber)/m_bucketSize;
			
//			for (int i = bucketNumber + 1; i <= m_buckets; i++) {
//				numElements += histogram.get(i);
//			}
//			numElements += (bucketNumber * m_bucketSize + m_min - v)*histogram.get(bucketNumber)/m_bucketSize;
//			//numElements += histogram.get(bucketNumber)/m_bucketSize;
//			numElements = m_totalElements - numElements;
			break;
		case EQUALS:
			numElements += histogram.get(bucketNumber)/m_bucketSize;
			break;
		case NOT_EQUALS:
			numElements = m_totalElements - histogram.get(bucketNumber)/m_bucketSize;
			break;
		}
		if (m_totalElements == 0)
			return 0;
		return (double) numElements / (double) m_totalElements;
	}

	/**
	 * @return
	 *     the average selectivity of this histogram.
	 *     
	 *     This is not an indispensable method to implement the basic
	 *     join optimization. It may be needed if you want to
	 *     implement a more efficient optimization
	 * */
	public double avgSelectivity()
	{
		return .10;
	}

	/**
	 * @return A string describing this histogram, for debugging purposes
	 */
	public String toString() {
		return histogram.toString();
	}
}
