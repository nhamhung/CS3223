package qp.operators;

import qp.utils.*;

import java.io.File;
import java.util.ArrayList;

public class Sort extends Operator {
    Operator base;                 // Base table to sort
    ArrayList<Attribute> attrset;  // Set of attributes to compare
    boolean isDistinct;
    TupleReader currentReader;
    int batchSize;                  // Number of tuples per page
    int numSubFiles;                // Number of sub files
    int numBuffers;                 // Number of buffers
    int numRounds;              // Number of merge rounds
    boolean endOfBase = false;      // Whether we've reached the end of the base operator
    boolean endOfSortedFile = false;     // Whether we've reached the end of the sorted file
    boolean sorted = false;         // Whether sorting have taken place, we only sort once
    boolean isDuplicatesRemoved = false;

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    Tuple prevTuple;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    ArrayList<Integer> attrIndex;

    public Sort(Operator base, ArrayList<Attribute> as, int type, boolean isDistinct) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.numSubFiles = 0;
        this.isDistinct = isDistinct;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumBuff(int num) {
        this.numBuffers = num;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;
        numRounds = 0;
        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggregation is not implemented.");
                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex.add(index);
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        if (!sorted) {
            createSubFiles();
            if (numSubFiles != 1) {
                sortSubFiles();
                isDuplicatesRemoved = true;
            }
            sorted = true;
        }
        if (currentReader == null) {
            currentReader = new TupleReader(String.format("tmp-%d-1", numRounds), batchSize);
            currentReader.open();
        }

        outbatch = getNextSortedPage();
        return outbatch;
    }

    private void createSubFiles() {
        while (!endOfBase) {
            Block newBlock = new Block(numBuffers, batchSize);
            // Create new sub file represented by block
            while (!newBlock.isFull()) {
                Batch nextPage = base.next();

                // nextPage is null when we've reached the end of base
                if (nextPage == null) {
                    endOfBase = true;
                    break;
                }

                if (isDistinct) {
                    nextPage = projectTuplesInPage(nextPage);
                }

                for (int i = 0; i < nextPage.size(); i++) {
                    newBlock.add(nextPage.get(i));
                }
            }

            // Sort the sub file
            newBlock.orderBy(attrIndex);

            // Write the sub file
            TupleWriter writer = new TupleWriter(String.format("tmp-0-%d", numSubFiles + 1), batchSize);
            writer.open();
            for (int i = 0; i < newBlock.size(); i++) {
                writer.next(newBlock.get(i));
            }
            writer.close();
            numSubFiles++;
        }
    }

    private void sortSubFiles() {
        while (numSubFiles != 1) {
            onePassMerge();
        }
    }

    private void onePassMerge() {
        int count = 0;
        int numSortPage = numBuffers - 1;
        while (numSubFiles > count * numSortPage) {
            int numFilesBeingRead = Math.min(numSubFiles - count * numSortPage, numSortPage);
            TupleReader[] readers = new TupleReader[numFilesBeingRead];
            // Initialise readers
            for (int i = 0;
                 i < numFilesBeingRead;
                 i++) {
                int fileNum = count * numSortPage + i + 1;
                readers[i] = new TupleReader(String.format("tmp-%d-%d", numRounds, fileNum), batchSize);
                readers[i].open();
            }
            // Create sorted file
            TupleWriter writer = new TupleWriter(String.format("tmp-%d-%d", numRounds + 1, count + 1), batchSize);
            writer.open();
            while (true) {
                Tuple newTuple = getNextTuple(readers);
                if (newTuple == null) break;
                if (isDistinct) {
                    if (prevTuple == null || Tuple.compareTuples(newTuple, prevTuple, attrIndex, attrIndex) != 0) {
                        writer.next(newTuple);
                        prevTuple = newTuple;
                    }
                } else {
                    writer.next(newTuple);
                }

                // No more values
            }
            writer.close();
            for (TupleReader reader : readers) {
                reader.close();
            }
            count = count + 1;
        }
        numSubFiles = count;
        numRounds += 1;
    }

    private Tuple getNextTuple(TupleReader[] readers) {
        Tuple nextTuple = null;
        int fileIndex = -1;
        for (int i = 0; i < readers.length; i++) {
            Tuple curTuple = readers[i].peek();
            if (curTuple == null) continue;
            if (nextTuple == null
                || Tuple.compareTuples(curTuple, nextTuple, attrIndex, attrIndex) < 0) { // curTuple < nextTuple
                nextTuple = curTuple;
                fileIndex = i;
            }
        }
        if (fileIndex != -1) {
            return readers[fileIndex].next();
        }
        return null;
    }

    private Batch getNextSortedPage() {
        if (endOfSortedFile) return null;
        Batch outputPage = new Batch(batchSize);
        while (!outputPage.isFull()) {
            Tuple nextTuple = currentReader.next();
            if (nextTuple == null) {
                endOfSortedFile = true;
                break;
            }
            if (isDistinct && !isDuplicatesRemoved) { // duplicates not yet removed from merging if onePassMerge() is not run
                if (prevTuple == null || Tuple.compareTuples(nextTuple, prevTuple, attrIndex, attrIndex) != 0) {
                    outputPage.add(nextTuple);
                    prevTuple = nextTuple;
                }
            } else {
                outputPage.add(nextTuple);
            }
        }
        return outputPage;
    }

    private Batch projectTuplesInPage(Batch page) {
        Batch projectedPage = new Batch(batchSize);
        for (int i = 0; i < page.size(); i++) {
            Tuple basetuple = page.get(i);
            ArrayList<Object> present = new ArrayList<>();
            for (int j = 0; j < attrset.size(); j++) {
                Object data = basetuple.dataAt(attrIndex.get(j));
                present.add(data);
            }
            Tuple outtuple = new Tuple(present);
            projectedPage.add(outtuple);
        }
        return projectedPage;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        cleanUpTmpFiles();
        return true;
    }

    private void cleanUpTmpFiles() {
        for (int i = 0; i <= numRounds; i += 1) {
            int j = 1;
            while (true) {
                File f = new File(String.format("tmp-%d-%d", i, j));
                if (!f.delete()) break;
                j += 1;
            }
        }
    }


    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Sort newSort = new Sort(newbase, newattr, optype, isDistinct);
        newSort.setNumBuff(numBuffers);
        newSort.setSchema(newbase.getSchema());
        return newSort;
    }
}

