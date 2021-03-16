/*
    Grace Hash Join algorithm
 */
package qp.operators;

import com.skjegstad.utils.*;
import org.apache.hive.common.util.*;

public class GraceHashJoin extends Join{
    public GraceHashJoin(Operator left, Operator right, int type) {
        super(left, right, type);
    }

}
