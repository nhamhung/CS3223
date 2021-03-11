/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.operators.Debug;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
    boolean isDistinct;

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    int inbatchindex;
    Batch outbatch;
    Tuple prevtuple;
    boolean isEndOfInputFile = false;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;
    ArrayList<Integer> attributeIndex;

    public Project(Operator base, ArrayList<Attribute> as, int type, boolean isDistinct) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.isDistinct = isDistinct;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        attributeIndex = new ArrayList<Integer>(attrset.size());
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
            attributeIndex.add(i, index);
        }
        return true;
    }

    public Batch loadAllInputToOutput() {
        inbatch = base.next();

        if (inbatch == null) {
            isEndOfInputFile = true;
            return null;
        }

        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);

            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex[j]);
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            outbatch.add(outtuple);
        }

        return outbatch;
    }

    public Batch loadDistinctInputToOutput() {
        while (!outbatch.isFull()) {
            if (inbatch == null || inbatchindex == inbatch.size()) {
                inbatch = base.next();
                if (inbatch == null || inbatch.size() == 0) {
                    isEndOfInputFile = true;
                    return outbatch;
                }
                inbatchindex = 0;
            }

            Tuple basetuple = inbatch.get(inbatchindex);
            if (prevtuple == null || Tuple.compareTuples(basetuple, prevtuple, attributeIndex, attributeIndex) != 0) {
                outbatch.add(getTupleGivenAttr(basetuple));
                prevtuple = basetuple;
            }

            inbatchindex++;
        }

        return outbatch;
    }

    public Tuple getTupleGivenAttr(Tuple basetuple) {
        ArrayList<Object> present = new ArrayList<>();
        for (int j = 0; j < attrset.size(); j++) {
            Object data = basetuple.dataAt(attrIndex[j]);
            present.add(data);
        }

        Tuple outtuple = new Tuple(present);
        return outtuple;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        if (isEndOfInputFile) {
            return null;
        }

        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/

        if (isDistinct) {
            loadDistinctInputToOutput();
        } else {
            loadAllInputToOutput();
        }

        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype, isDistinct);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
