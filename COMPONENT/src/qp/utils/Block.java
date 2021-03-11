/**
 * Batch represents a page
 **/

package qp.utils;

import java.util.ArrayList;
import java.io.Serializable;
import java.util.Collections;

public class Block implements Serializable {

    int totalCapacity;          // Total number of tuples that can fit in the block
    int numBuffers;             // Number of buffers per block
    ArrayList<Tuple> tuples;    // The tuples in the page

    /** Number of tuples per page **/
    public Block(int numBuffers, int tuplesPerPage) {
        this.numBuffers = numBuffers;
        totalCapacity = numBuffers * tuplesPerPage;
        tuples = new ArrayList<>(totalCapacity);
    }

    public void orderBy(ArrayList<Integer> attrs, Boolean isDesc) {
        int isDescFactor = isDesc ? -1 : 1;
        tuples.sort((t1, t2) -> isDescFactor * Tuple.compareTuples(t1, t2, attrs, attrs));
    }

    /** Insert the record in page at next free location **/
    public void add(Tuple t) {
        tuples.add(t);
    }

    /** @return number of tuples that the block can hold **/
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

    /** Returns the number of tuples in the block **/
    public int size() {
        return tuples.size();
    }

    /** Returns true if number of typles in block **/
    public boolean isFull() {
        return size() == capacity();
    }
}