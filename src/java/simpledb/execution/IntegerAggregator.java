package simpledb.execution;

import java.util.*;
import simpledb.common.Type;
import simpledb.storage.Tuple;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        throw new UnsupportedOperationException("please implement me for lab2");
    }

    private class intAggIterator {
    private int aggregate(ArrayList<integer> elems){
        //can not aggregate on empty elements
        if (elems.length()==0){
            throw new UnsupportedOperationException("expected atleast one element");
        }
            int result = 0;
            switch (what) {
                case MIN:
                //get the very first elems
                    result=elems.get(0);
                    for (int i:elems){
                        if i<result:
                        result=i;
                    }
                    break;
                case MAX:
                    result=elems.get(0);
                    for (int i:elems){
                        if i>result:
                        result=i;
                    }
                    break;
                    //SUM , AVG , MIN , MAX

                case SUM:
                    result=0;
                    for (int i:elems){
                        result=result+i;
                    }
                    break;
                case AVG:
                    int tempsum=0;
                    for (int i:elems){
                        tempsum=tempsum+i;
                    }
                    result=tempsum/elems.length();
                    break;
                case COUNT:
                    result=elems.length();
                    break;
                default:
                    throw new notImplementedException();
            }
        }
    



    }
}