import com.couchbase.client.java.Bucket;
import com.couchbase.spark.java.CouchbaseSparkContext;
import org.apache.spark.SparkContext;
import workload.KVWorkload;
import workload.WorkloadAdmin;

/**
 * Created by Subhashni on 9/28/15.
 */
public class Tests {
    private String[] nodes = null;
    private String master = null;
    private Bucket couchbaseBucket = null;
    private SparkContext sc = null;
    private CouchbaseAdmin admin = null;
    private int waitTime = 10000;
    private WorkloadAdmin kvwlAdmin = null;
    private WorkloadAdmin n1qlwlAdmin = null;
    private WorkloadAdmin saveTocbAdmin = null;

    public Tests (Bucket couchbaseBucket, SparkContext sc, String[] nodes, CouchbaseAdmin admin) {
        this.nodes = nodes;
        this.master = nodes[0];
        this.couchbaseBucket = couchbaseBucket;
        this.sc = sc;
        this.admin = admin;
        this.kvwlAdmin = new WorkloadAdmin(new KVWorkload(this.couchbaseBucket, this.sc));
    }

    String[] failoverNodes(Boolean force) {
        int count = nodes.length / 2;
        String[] removeNodes = new String[count];
        int i = 0;
        while (i < count) {
            removeNodes[i] = nodes[i + 1];
            i++;
        }
        System.out.print(removeNodes[0]);
        System.out.print(this.master);
        String ServerRemove = "";
        for (String node : removeNodes) {
            ServerRemove += " --server-failover=" + node + ":8091";
        }
        String command = "/opt/couchbase/bin/couchbase-cli failover" + ServerRemove + " -c " + this.master + ":8091 -u Administrator -p password";
        if (force) {
            command += " --force";
        }
        this.admin.runCommand(command);
        return removeNodes;
    }

    String[] rebalanceOutNodes() {
        int count = nodes.length /2 ;
        String[] removeNodes =  new String[count];
        int i=0;
        while (i < count) {
            removeNodes[i] = nodes[i + 1];
            i++;
        }
        System.out.print(removeNodes[0]);
        System.out.print(this.master);
        String ServerRemove = "";
        for (String node: removeNodes) {
            ServerRemove += " --server-remove=" + node +":8091";
        }
        String command = "/opt/couchbase/bin/couchbase-cli rebalance" + ServerRemove + " -c " + this.master +":8091 -u Administrator -p password";
        this.admin.runCommand(command);
        return removeNodes;
    }

    void rebalanceInNodes(String[] addNodes) {
        String ServerAdd = "";
        for (String node: addNodes) {
            ServerAdd += " --server-add=" + node + ":8091 --server-add-username=Administrator --server-add-password=password --services=data,index,query";
        }
        String command =  "/opt/couchbase/bin/couchbase-cli server-add" + ServerAdd + " -c " + this.master +":8091 -u Administrator -p password";
        this.admin.runCommand(command);

        command = "/opt/couchbase/bin/couchbase-cli rebalance -c " + this.master +":8091 -u Administrator -p password";
        this.admin.runCommand(command);
    }

    void rebalanceStatus() {
        String command = "/opt/couchbase/bin/couchbase-cli rebalance-status -c " + this.master +":8091 -u Administrator -p password";
        this.admin.runCommand(command);
    }

    public void TestKVRebalanceOut() {
        kvwlAdmin.startWorkload();
        String[] removedNodes = rebalanceOutNodes();

        try {
            kvwlAdmin.stopWorkload();
            Thread.sleep(waitTime);
        } catch (InterruptedException ex) {

        }
        //readd nodes
        rebalanceInNodes(removedNodes);
    }

    public void TestKVGracefulFailover() {
        kvwlAdmin.startWorkload();
        String[] failedoverNodes = failoverNodes(false);
        try{
            kvwlAdmin.stopWorkload();
            Thread.sleep(waitTime);
        } catch(InterruptedException ex) {
        }
        rebalanceInNodes(failedoverNodes);
    }

    public void TestKVHardFailover() {
        kvwlAdmin.startWorkload();
        String[] failedoverNodes = failoverNodes(true);
        try{
            kvwlAdmin.stopWorkload();
            Thread.sleep(waitTime);
        } catch(InterruptedException ex) {

        }
        rebalanceInNodes(failedoverNodes);
    }



}
