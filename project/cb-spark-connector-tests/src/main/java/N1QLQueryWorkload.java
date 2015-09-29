import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.QueryRow;
import com.couchbase.spark.java.CouchbaseSparkContext;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.tools.cmd.gen.AnyVals;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Subhashni on 9/28/15.
 */
public class N1QLQueryWorkload implements Runnable {
    private String key_prefix = "TestKeyN1QL";
    private String value_prefix = "TestValueN1QL";
    private String n1qlField = "N1QLValue";
    private CouchbaseSparkContext csc = null;
    private Bucket couchbaseBucket = null;
    private boolean shouldRun = true;
    private int waitTime = 1000;
    private int kvCount = 100;

    public N1QLQueryWorkload (Bucket couchbaseBucket, CouchbaseSparkContext csc) {
        this.csc = csc;
        this.couchbaseBucket = couchbaseBucket;
    }

    public void preload() {
        for (int i=0; i< kvCount; i++){
            String key = key_prefix + i;
            JsonObject obj = JsonObject.create();
            obj.put(n1qlField, value_prefix+i);
            this.couchbaseBucket.upsert(JsonDocument.create(key, obj), PersistTo.MASTER);
            try {
                Thread.sleep(100);
            }catch (InterruptedException ex) {

            }
        }
        Query q = Query.simple("CREATE PRIMARY INDEX ON `DEFAULT`");
        this.couchbaseBucket.query(q);
    }

    public void run() {
        int i= 0;
        while(this.shouldRun) {
            Query q = Query.simple("select * from `DEFAULT` where "+ n1qlField +" is not null");
            List<CouchbaseQueryRow> docs = this.csc
                    .couchbaseQuery(q, "default")
                    .collect();

            try {
                Thread.sleep(waitTime);
            } catch(InterruptedException ex) {
                System.out.print("Exception on N1QL thread" + ex.getMessage());
                System.exit(1);
            }
            assert (docs.size() == kvCount);
        }
    }

    public void initialize() {
        this.shouldRun = true;
    }

    public void stop() {
        this.shouldRun = false;
    }

}
