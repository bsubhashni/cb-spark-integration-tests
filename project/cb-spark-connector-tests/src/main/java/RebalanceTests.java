import com.couchbase.client.java.Bucket;
import com.couchbase.spark.java.CouchbaseSparkContext;

/**
 * Created by Subhashni on 9/28/15.
 */
public class RebalanceTests {
    public static String[] nodes = null;
    public static String master = null;
    public static Bucket couchbaseBucket = null;
    public static CouchbaseSparkContext csc = null;

    public RebalanceTests (Bucket couchbaseBucket, CouchbaseSparkContext csc, String[] nodes) {
        this.nodes = nodes;
        this.master = nodes[0];
        this.couchbaseBucket = couchbaseBucket;
        this.csc = csc;
    }

    String[] rebalanceOutNodes() {
        int count = nodes.length /2 ;
        String[] removeNodes =  new String[count];
        while (removeNodes.length < count) {
            removeNodes[removeNodes.length] = nodes[removeNodes.length + 1];
        }

        return removeNodes;
    }

    void rebalanceInNodes(String[] addNodes) {

    }

    public void TestKVRebalanceOut() {
        KVWorkload kvWorkload = new KVWorkload(couchbaseBucket, csc);
        Thread th = new Thread(kvWorkload);
        th.start();
        String[] removedNodes = rebalanceOutNodes();
        rebalanceInNodes(removedNodes);

        try {
            Thread.sleep(20);
            th.join();
        } catch (InterruptedException ex) {

        }
    }


}
