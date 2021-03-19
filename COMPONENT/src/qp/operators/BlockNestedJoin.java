package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchSize;                  // Number of tuples per out batch
    ArrayList<Integer> leftIndex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightIndex;  // Indices of the join attributes in right table
    ArrayList<Integer> condOps;     // Indicates the type of comparison for join
    String rfname;                  // The file name where the right table is materialized
    Batch outputPage;                 // Buffer page for output
    Block leftBlock;                // Buffer block for left input stream
    Batch rightPage;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int leftCursor;                       // Cursor for left side buffer
    int rightCursor;                      // Cursor for right side buffer
    boolean endOfLeftTable;                    // Whether end of stream (left table) is reached
    boolean endOfRightTable;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     * Kept the same as NestedJoin
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tupleSize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tupleSize;

        /** find indices attributes of join conditions **/
        leftIndex = new ArrayList<>();
        rightIndex = new ArrayList<>();
        condOps = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftAttr = con.getLhs();
            Attribute rightAttr = (Attribute) con.getRhs();
            leftIndex.add(left.getSchema().indexOf(leftAttr));
            rightIndex.add(right.getSchema().indexOf(rightAttr));
            condOps.add(con.getExprType());
        }
        Batch rightPage;

        /** initialize the cursors of input buffers **/
        leftCursor = 0;
        rightCursor = 0;
        endOfLeftTable = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        endOfRightTable = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "NJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightPage = right.next()) != null) {
                    out.writeObject(rightPage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("NestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        if (endOfLeftTable) { //  If end of left table, return null
            return null;
        }
        outputPage = new Batch(batchSize);
        while (!endOfLeftTable) {
            // If left block is not initialised or we've reached the end of the right file,
            // need to read in left block and reinitialise
            if (leftBlock == null || endOfRightTable) {
                // read in left block
                readLeftBlock();

                // Whenever a new left block comes, start the scanning of right table
                initializeRightTable();
            }

            // Nested loop
            while (!endOfRightTable) {
                if (rightCursor == 0 && leftCursor == 0) {
                    readRightPage();
                }
                while (leftCursor < leftBlock.size()) {
                    while (rightCursor < rightPage.size()) {
                        Tuple leftTuple = leftBlock.get(leftCursor);
                        Tuple rightTuple = rightPage.get(rightCursor);
                        if (leftTuple.checkJoin(rightTuple, leftIndex, rightIndex, condOps)) {
                            Tuple outTuple = leftTuple.joinWith(rightTuple);
                            outputPage.add(outTuple);
                            if (outputPage.isFull()) {
                                rightCursor++;
                                return outputPage;
                            }
                        }
                        rightCursor++;
                    }
                    rightCursor = 0;
                    leftCursor++;
                }
                leftCursor = 0;
            }
        }
        return outputPage;
    }

    private void initializeRightTable() {
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            endOfRightTable = false;
        } catch (IOException io) {
            System.err.println("NestedJoin:error in reading the file");
            System.exit(1);
        }
    }

    private void readRightPage() {
        try {
            rightPage = (Batch) in.readObject();
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error in reading temporary file");
            }
            rightPage = new Batch(0);
            endOfRightTable = true;
        } catch (ClassNotFoundException c) {
            System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("BlockNestedJoin: Error in reading temporary file");
            System.exit(1);
        }
    }

    private void readLeftBlock() {
        int numberOfTuplesPerPage = Batch.getPageSize() / left.schema.getTupleSize();
        Block leftBlock = new Block(numBuff - 2, numberOfTuplesPerPage);
        while (!leftBlock.isFull()) {
            Batch leftBatch = left.next();
            if (leftBatch == null) {
                break;
            }
            for (int i = 0; i < leftBatch.size(); i++) {
                leftBlock.add(leftBatch.get(i));
            }
        }
        this.leftBlock = leftBlock;
        if (leftBlock.isEmpty()) {
            endOfLeftTable = true;
        }
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }
}