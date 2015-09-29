import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import java.util.Arrays;
import java.util.List;
import com.couchbase.spark.java.CouchbaseSparkContext;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.Bucket;
/**
 * Created by Subhashni on 9/28/15.
 */
public class KVWorkload implements Runnable {
    private String key_prefix = "TestKey";
    private String value_prefix = "TestValue";
    private CouchbaseSparkContext csc = null;
    private Bucket couchbaseBucket = null;
    private boolean shouldRun = true;
    private int waitTime = 1000;

    public KVWorkload (Bucket couchbaseBucket, CouchbaseSparkContext csc) {
        this.csc = csc;
        this.couchbaseBucket = couchbaseBucket;
    }

    public void run() {
        int i= 0;
        while(this.shouldRun) {
            String key = key_prefix + i;
            JsonObject obj = JsonObject.create();
            obj.put("Value", value_prefix+i);
            this.couchbaseBucket.upsert(JsonDocument.create(key, obj), PersistTo.MASTER);
            List<JsonDocument> docs = this.csc
                    .couchbaseGet(Arrays.asList(key))
                    .collect();
            try {
                Thread.sleep(waitTime);
            } catch(InterruptedException ex) {
                System.out.print("Exception on KVWorkload thread" + ex.getMessage());
                System.exit(1);
            }
            assert(docs.size() == 1);
            i++;
        }
    }

    public void initialize() {
        this.shouldRun = true;
    }

    public void stop() {
        this.shouldRun = false;
    }

}
