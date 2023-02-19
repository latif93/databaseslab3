package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ArrayList<Integer> integerAggregator;
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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here

        if(gbfield == NO_GROUPING){
            integerAggregator.add(((IntField)tup.getField(afield)).getValue());
        }
        else if (gbfieldtype == Type.INT_TYPE){
            HashMap<Integer, ArrayList<Integer>> group_intAggregator = new HashMap<Integer, ArrayList<Integer>>() ;
            Integer group_key = (((IntField)tup.getField(gbfield)).getValue());
            Integer aggr_val = (((IntField) tup.getField(afield)).getValue());
            if(!group_intAggregator.containsKey(group_key)) {
                group_intAggregator.put(group_key, new ArrayList<Integer>(1));
            }
            group_intAggregator.get(group_key).add(aggr_val);
        }
        else if(gbfieldtype == Type.STRING_TYPE){
            HashMap<String, ArrayList<Integer>> group_intAggregator = new HashMap<String, ArrayList<Integer>>() ;
            String group_key = (((StringField)tup.getField(gbfield)).getValue());
            Integer aggr_val = (((IntField) tup.getField(afield)).getValue());
            if(!group_intAggregator.containsKey(group_key)) {
                group_intAggregator.put(group_key, new ArrayList<Integer>(1));
            }
            group_intAggregator.get(group_key).add(aggr_val);
        }
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
        return new intAggIterator();
    }
    private class intAggIterator implements OpIterator{
        private int aggregate(ArrayList<Integer> elems) throws DbException {
            //can not aggregate on empty elements
            if (elems.size()==0){
                throw new UnsupportedOperationException("expected atleast one element");
            }
            int result = 0;
            switch (what) {
                case MIN:
                    //get the very first elems
                    result=elems.get(0);
                    for (int i:elems){
                        if(i<result){
                        result=i;
                        }
                    }
                    break;
                case MAX:
                    result=elems.get(0);
                    for (int i:elems){
                        if(i>result){
                            result=i;
                        }
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
                    result=tempsum/elems.size();
                    break;
                case COUNT:
                    result=elems.size();
                    break;
                default:
                    throw new DbException("Unsupported operator");
            }
            return result;
        }
        public void open(){
            this.open();
        }
        public boolean hasNext(){
            this.open();
        }
        public void next(){}
        public void rewind() throws DbException, TransactionAbortedException {}
        public void getTupleDesc(){}
        public void close(){}

    }

}
