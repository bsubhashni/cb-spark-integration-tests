import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.CouchbaseCluster;
import java.util.Arrays;
import java.util.List;
import com.couchbase.spark.java.CouchbaseSparkContext;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.Bucket;
/**
 * Created by Subhashni on 9/28/15.
 */
public class KVWorkload implements Runnable {
    public static String key_prefix = "TestKey";
    public static String value_prefix = "TestValue";
    public static CouchbaseSparkContext csc = null;
    public static Bucket couchbaseBucket = null;

    public KVWorkload (Bucket couchbaseBucket, CouchbaseSparkContext csc) {
        this.csc = csc;
        this.couchbaseBucket = couchbaseBucket;
    }

    public void run() {
        int i= 0;
        while(true) {
            String key = key_prefix + i;
            JsonObject obj = JsonObject.create();
            obj.put("Value", value_prefix+i);
            this.couchbaseBucket.insert(JsonDocument.create(key,obj), PersistTo.MASTER);
            List<JsonDocument> docs = this.csc
                    .couchbaseGet(Arrays.asList(key))
                    .collect();
            assert(docs.size() == 1);
        }
    }

}
