/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    // Set initial numBuffer = 10, minimum would be 3
    // This is to support sort if there are no joins
    static int numBuffer = 10;
    static int numJoin;

    static int buffPerJoin;

    public BufferManager(int numBuffer, int numJoin) {
        this.numBuffer = numBuffer;
        this.numJoin = numJoin;
        buffPerJoin = numBuffer / numJoin;
    }

    public static int getBuffersPerJoin() {
        return buffPerJoin;
    }

}
