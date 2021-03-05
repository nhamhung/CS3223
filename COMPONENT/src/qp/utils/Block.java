/**
 * Batch represents a page
 **/

package qp.utils;

import java.util.ArrayList;
import java.io.Serializable;

public class Block implements Serializable {

    int totalCapacity;          // Max capacity of the block
    int numBuffers;             // Number of buffers per block
    static int BlockSize;       // The literal size of each block
    ArrayList<Tuple> tuples;    // The tuples in the page

    /** Set number of bytes per page **/
    // TODO: Set block size if needed
    public static void setBlockSize(int size) {
        BlockSize = size;
    }

    /** Get number of bytes per page **/
    public static int getBlockSize() {
        return BlockSize;
    }

    /** Number of tuples per page **/
    public Block(int numPages, int tuplesPerPage) {
        numBuffers = numPages;
        totalCapacity = numPages * tuplesPerPage;
        tuples = new ArrayList<>(totalCapacity);
    }

    /** Insert the record in page at next free location **/
    public void add(Tuple t) {
        tuples.add(t);
    }

    public int capacity() {
        return totalCapacity;
    }

    public void clear() {
        tuples.clear();
    }

    public boolean contains(Tuple t) {
        return tuples.contains(t);
    }

    public Tuple get(int i) {
        return tuples.get(i);
    }

    public int indexOf(Tuple t) {
        return tuples.indexOf(t);
    }

    public void add(Tuple t, int i) {
        tuples.add(i, t);
    }

    public boolean isEmpty() {
        return tuples.isEmpty();
    }

    public void remove(int i) {
        tuples.remove(i);
    }

    public void set(Tuple t, int i) {
        tuples.set(i, t);
    }

    public int size() {
        return tuples.size();
    }

    public boolean isFull() {
        if (size() == capacity())
            return true;
        else
            return false;
    }
}