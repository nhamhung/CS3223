package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchSize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Block leftBlock;                // Buffer block for left input stream
    Batch rightBatch;               // Buffer page for right input stream
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
        int tuplesize = schema.getTupleSize();
        batchSize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

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
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
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
        int i, j;
        if (endOfLeftTable) {
            return null;
        }
        outbatch = new Batch(batchSize);
        while (!outbatch.isFull()) {
            // If left cursor is at 0 or end of right file
            if (leftCursor == 0 && endOfRightTable) {
                // new left block much bigger
                leftBlock = createLeftBlock();
                if (leftBlock.isEmpty()) {
                    endOfLeftTable = true;
                    return outbatch;
                }
                // Whenever a new left block came, we have to start the scanning of right table
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    endOfRightTable = false;
                } catch (IOException io) {
                    System.err.println("NestedJoin:error in reading the file");
                    System.exit(1);
                }

            }
            while (!endOfRightTable) {
                try {
                    if (rightCursor == 0 && leftCursor == 0) {
                        rightBatch = (Batch) in.readObject();
                    }
                    for (i = leftCursor; i < leftBlock.size(); ++i) {
                        for (j = rightCursor; j < rightBatch.size(); ++j) {
                            Tuple leftTuple = leftBlock.get(i);
                            Tuple rightTuple = rightBatch.get(j);
                            if (leftTuple.checkJoin(rightTuple, leftindex, rightindex)) {
                                Tuple outtuple = leftTuple.joinWith(rightTuple);
                                outbatch.add(outtuple);
                                if (outbatch.isFull()) {
                                    if (i == leftBlock.size() - 1 && j == rightBatch.size() - 1) {  //case 1
                                        leftCursor = 0;
                                        rightCursor = 0;
                                    } else if (i != leftBlock.size() - 1 && j == rightBatch.size() - 1) {  //case 2
                                        leftCursor = i + 1;
                                        rightCursor = 0;
                                    } else if (i == leftBlock.size() - 1 && j != rightBatch.size() - 1) {  //case 3
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    } else {
                                        leftCursor = i;
                                        rightCursor = j + 1;
                                    }
                                    return outbatch;
                                }
                            }
                        }
                        rightCursor = 0;
                    }
                    leftCursor = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    endOfRightTable = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

    private Block createLeftBlock() {
        int numberOfTuplesPerPage = Batch.getPageSize() / left.schema.getTupleSize();
        Block leftBlock = new Block(numBuff, numberOfTuplesPerPage);
        while (!leftBlock.isFull()) {
            Batch leftBatch = left.next();
            if (leftBatch == null) {
                break;
            }
            for (int i = 0; i < leftBatch.size(); i++) {
                leftBlock.add(leftBatch.get(i));
            }
        }
        return leftBlock;
    }
}
