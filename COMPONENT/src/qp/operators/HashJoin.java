/*
    Grace Hash Join algorithm
 */
package qp.operators;

import bloom.BloomFilter;
import murmur.Murmur3;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Random;

import qp.utils.*;

public class HashJoin extends Join{
    static int filenum = 0;         // To get unique filenum for this operation

    int batchSize;                  // Number of tuples per out batch
    int leftBatchMaxSize;              // Number of tuples per batch for left table
    int rightBatchMaxSize;             // Number of tuples per batch for right table

    ArrayList<Integer> leftIndex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightIndex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outputPage;                 // Buffer page for output
    Block leftBlock;                // Buffer block for left input stream
    Batch rightPage;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int count = 0;

    int leftCursor;                       // Cursor for left side buffer
    int rightCursor;                      // Cursor for right side buffer
    boolean endOfLeftTable = true;                    // Whether end of stream (left table) is reached
    boolean endOfRightTable = true;                   // Whether end of stream (right table) is reached
    Batch[] buckets;
    Batch[] inMemoryBuckets;
    ObjectInputStream rightInputStream;
    ObjectInputStream leftInputStream;
    String leftInputStreamName = "";
    String rightInputStreamName = "";
    Batch leftBatch;
    Batch rightBatch;
    int partitionCounter = -1;
    int N;
    int joinid;

    BloomFilter<Integer> bloomFilter;

    public HashJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        System.out.println(leftIndex);
        /*
            This join makes use of temporary output files.
            When multiple hash joins happen concurrently we need an identifier to differentiate files that are
            spawned by different joins.
         */
        joinid = this.hashCode();
    }

    /*
      Hash function using the Murmur3 algorithm.
      noOfBuckets is supplied so this function returns the bucket number, to be precise.
     */
    private int hash(Tuple record, int noOfBuckets, ArrayList<Integer> index, String sep){
        StringBuilder sb = new StringBuilder();
        for (Integer i : index) sb.append(record.dataAt(i)).append(sep);
        int k = Murmur3.hash32(sb.toString().getBytes(StandardCharsets.UTF_8)) % noOfBuckets;
        return (k >= 0) ? k : -k;
    }

    /*
     Phase 1: partitions LEFT and RIGHT into buckets.
     */
    public boolean open() {
        /* Number of pages for partitioning */
        N = numBuff - 1;


        /* select number of tuples per batch */
        batchSize = Batch.getPageSize() / schema.getTupleSize();
        leftBatchMaxSize = Batch.getPageSize() / left.schema.getTupleSize();
        rightBatchMaxSize = Batch.getPageSize() / right.schema.getTupleSize();

        /* find indices attributes of join conditions */
        leftIndex = new ArrayList<>();
        rightIndex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftAttr = con.getLhs();
            Attribute rightAttr = (Attribute) con.getRhs();
            leftIndex.add(left.getSchema().indexOf(leftAttr));
            rightIndex.add(right.getSchema().indexOf(rightAttr));
        }
        Batch rightPage;
        Batch leftPage;


        /* initialize the cursors of input buffers */
        leftCursor = 0;
        rightCursor = 0;
        endOfLeftTable = endOfRightTable = true;

        /* Initialize bucket array, bloom filter, and seed for MurmurHash */
        buckets = new Batch[N];
        bloomFilter = new BloomFilter<Integer>(32, N, 4);

        /* Partitioning left then right table. Using bloom filter to eliminate guaranteed mismatch. */

        if (!left.open()) {
            return false;
        } else {
            try {
                ObjectOutputStream[] leftOutputStreams = new ObjectOutputStream[N];
                for (int i = 0; i < N; i++) {
                    buckets[i] = new Batch(leftBatchMaxSize);
                    leftOutputStreams[i] = new ObjectOutputStream(new FileOutputStream("L" + i + joinid));
                }
                while ((leftPage = left.next()) != null) {
                    while (leftCursor < leftPage.size()) {
                        Tuple record = leftPage.get(leftCursor);
                        int k = hash(record, N, leftIndex, "%");
                        bloomFilter.add(k);
                        if (buckets[k].size() >= leftBatchMaxSize) {
                            leftOutputStreams[k].writeObject(buckets[k]);
                            buckets[k] = new Batch(leftBatchMaxSize);
                        }
                        buckets[k].add(record);
                        leftCursor++;
                    }
                    leftCursor = 0;
                }
                for (int i = 0; i < N; i++) {
                    if (!buckets[i].isEmpty()) leftOutputStreams[i].writeObject(buckets[i]);
                    leftOutputStreams[i].close();
                }
                leftCursor = 0;
            } catch (IOException e) {
                return false;
            }
        }

        if (!right.open()) {
            return false;
        } else {
            try {
                ObjectOutputStream[] rightOutputStreams = new ObjectOutputStream[N];
                for (int i = 0; i < N; i++) {
                    buckets[i] = new Batch(rightBatchMaxSize);
                    rightOutputStreams[i] = new ObjectOutputStream(new FileOutputStream("R" + i + joinid));
                }
                while ((rightPage = right.next()) != null) {
                    while (rightCursor < rightPage.size()) {
                        Tuple record = rightPage.get(rightCursor);
                        // rightCursor has to be here
                        rightCursor++;
                        int k = hash(record, N, rightIndex, "%");
                        /* Zero false negative at its finest*/
                        if (!bloomFilter.contains(k)) continue;
                        if (buckets[k].size() >= rightBatchMaxSize) {
                            rightOutputStreams[k].writeObject(buckets[k]);
                            buckets[k] = new Batch(rightBatchMaxSize);
                        }
                        buckets[k].add(record);
                    }
                    rightCursor = 0;
                }
                for (int i = 0; i < N; i++) {
                    if (!buckets[i].isEmpty()) rightOutputStreams[i].writeObject(buckets[i]);
                    rightOutputStreams[i].close();
                }
                rightCursor = 0;

                /*
                    Reinitialize the bloom filter.
                    This time with more hash functions.
                    This filter is used for the next() operations.
                 */
                bloomFilter = new BloomFilter<Integer>(32, N, 6);
            } catch (IOException e) {
                System.out.println("[Partition] Error while partitioning left table.");
                return false;
            }
        }
        return right.close() && left.close();

    }

    private void openPartitionInputStreams() throws IOException {
        /*
         * Identify the partitions of interest.
         * Reaching end of left table means we can go to the next partition.
         * Otherwise, we stay with the current partition number.
         */
        if (endOfLeftTable) {
            partitionCounter++;
            leftInputStreamName = "L" + partitionCounter + joinid;
            rightInputStreamName = "R" + partitionCounter + joinid;
        }

        /*
         * Create input stream to read from the left and right partitions of the selected number.
         */
        leftInputStream = new ObjectInputStream(new FileInputStream(leftInputStreamName));
        rightInputStream = new ObjectInputStream(new FileInputStream(rightInputStreamName));

    }

    private void readLeftTablePartition() {

        boolean hasBucketOverflow = false;

        while (!endOfLeftTable && !hasBucketOverflow) {
            try {
                leftBatch = (Batch) leftInputStream.readObject();
                while (leftBatch == null || leftBatch.isEmpty())
                    leftBatch = (Batch) leftInputStream.readObject();
            } catch (EOFException eof) {
                endOfLeftTable = true;
                try {
                    leftInputStream.close();
                } catch (Exception inputE) {
                    inputE.printStackTrace();
                }
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }


            /*
             * Read from the left partition
             */
            for (int i = 0; i < leftBatch.size(); i++) {
                            /*
                                Here, changing the separator allows us to generate a different hash function
                             */
                Tuple record = leftBatch.get(i);
                int k = hash(record, N, leftIndex, "?");
                bloomFilter.add(k);
                inMemoryBuckets[k].add(record);
                /*
                    Bucket overflows so:
                        Stop reading left partition.
                        Write back the unread records.
                        Join what we have so far with the right partition.
                 */
                if (inMemoryBuckets[k].size() >= leftBatchMaxSize) {
                    hasBucketOverflow = true;
                    endOfLeftTable = false;
                    String temp = "temp" + joinid;
                    try {
                        ObjectOutputStream tempOutputStream = new ObjectOutputStream(new FileOutputStream(temp));
                        /*
                            If we finish the current batch then all is well.
                            Otherwise, all unfinished records have to be kept
                         */
                        if (i < leftBatch.size() - 1) {
                            for (int j = 0; j <= i; j++) leftBatch.remove(0);
                            tempOutputStream.writeObject(leftBatch);
                        }
                        while (true) {
                            try {
                                tempOutputStream.writeObject(leftInputStream.readObject());
                            } catch (EOFException eof) {
                                leftInputStream.close();
                                tempOutputStream.close();
                                break;
                            }
                        }
                        /*
                           Rename temp file to partition file to say I/O
                         */
                        File leftInputFile = new File(leftInputStreamName);
                        leftInputFile.delete();
                        File tempOutputFile = new File(temp);
                        while (true) {
                            boolean couldRename;
                            try {
                                couldRename = tempOutputFile.renameTo(leftInputFile);
                            } catch (Exception e) {
                                e.printStackTrace();

                                // If renaming does not work then we need to transfer from temp file over
                                ObjectOutputStream leftOutput =
                                        new ObjectOutputStream(new FileOutputStream(leftInputStreamName));
                                ObjectInputStream tempInput =
                                        new ObjectInputStream(new FileInputStream(tempOutputFile));
                                while (true) {
                                    try {
                                        //write data from temp file to new file
                                        leftOutput.writeObject(tempInput.readObject());
                                    } catch (EOFException io) {
                                        tempInput.close();
                                        leftOutput.close();
                                        tempOutputFile.delete();
                                        break;
                                    }
                                }
                                break;
                            }
                            if (couldRename) break;
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    // Don't need to read in anymore once a bucket overflows.
                    break;
                }
            }

        }
    }

    /*
     Phase 2: for each partition number, check records from the left and right table.
     */
    public Batch next() {
        System.out.println("Doing " + joinid);
        /* Number of pages available 
            = numBuffs - 2 because one is for output buffer
                and one is for the probing partition
         */
        N = numBuff - 2;
        outputPage = new Batch(batchSize);
        while (!outputPage.isFull()) {
            /*
             *  If reach the end of both partitions and the cursors are reset then:
             *  1. Need to dispense the output buffer content
             *  2. If partitions are all processed then the join completes
             */
            if (leftCursor == 0 && rightCursor == 0 && endOfRightTable) {
                if (partitionCounter == N && endOfLeftTable) {
                    // flush the buffer
                    if (!outputPage.isEmpty()) return outputPage;
                    return null;
                } else {
                    try {
                        openPartitionInputStreams();
                    } catch (IOException e) {
                        e.printStackTrace();
                        continue;
                    }
                    /*
                     * Initialize the hashtable in-memory that we will store the left partition in.
                     * Again, ONLY numBuffs - 2 buffers are available to store this hashtable.
                     */
                    inMemoryBuckets = new Batch[N];
                    for (int i = 0; i < N; i++) inMemoryBuckets[i] = new Batch(leftBatchMaxSize);
                    endOfLeftTable = false;
                    endOfRightTable = false;
                    rightBatch = null;
                    readLeftTablePartition();
                    /*
                        Read in one batch from the right table.
                     */
                    try {
                        rightBatch = (Batch) rightInputStream.readObject();
                    } catch (EOFException eof) {
                        // This partition number is done.
                        endOfRightTable = true;
                        leftCursor = 0;
                        rightCursor = 0;
                        try {
                            rightInputStream.close();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                }
            }
            while (!endOfRightTable) {

                for (int i = rightCursor; i < rightBatch.size(); i++) {
                    Tuple rightRecord = rightBatch.get(i);
                    int k = hash(rightRecord, N, rightIndex, "?");
                    /*
                       Bloom filter black magic again.
                     */
                    if (bloomFilter.contains(k)) {
                        for (int j = leftCursor; j < inMemoryBuckets[k].size(); j++) {
                            Tuple leftRecord = inMemoryBuckets[k].get(j);
                            if (leftRecord.checkJoin(rightRecord, leftIndex, rightIndex)) {
                                Tuple record = leftRecord.joinWith(rightRecord);
                                outputPage.add(record);
                                // If output buffer is full, spill to disk
                                if (outputPage.isFull()) {
                                    /* If the bucket is exhausted we can move on to the next entry in the right table
                                    and reset pointer to the left table.
                                       Otherwise, continue looking
                                    */
                                    if ((j == inMemoryBuckets[k].size() - 1) && (i != rightBatch.size())) {
                                        leftCursor = 0;
                                        rightCursor = i + 1;
                                    } else {
                                        leftCursor = j + 1;
                                        rightCursor = i;
                                    }
                                    return outputPage;
                                }
                            }
                        }
                    }
                    leftCursor = 0;
                }
                rightCursor = 0;
                try {
                    rightBatch = (Batch) rightInputStream.readObject();
                    while (rightBatch == null || rightBatch.isEmpty())
                        rightBatch = (Batch) rightInputStream.readObject();
                } catch (EOFException eof) {
                    endOfRightTable = true;
                    try {
                        rightInputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        System.exit(1);
                    }
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
        return outputPage;

    }
    /*
     * Close the operator
     */
    public boolean close() {
       for (int i = 0; i < numBuff - 1; i++) {
            (new File("L"+ i+ joinid)).delete();
            (new File("R"+ i+ joinid)).delete();
       }
       return true;
    }

}
