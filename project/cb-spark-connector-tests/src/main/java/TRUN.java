import com.couchbase.client.java.*;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.spark.java.CouchbaseSparkContext;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import static com.couchbase.spark.java.CouchbaseSparkContext.couchbaseContext;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.IOException;
import com.google.gson.Gson;
/**
 * Created by Subhashni on 9/28/15.
 */
public class TRUN {
    ArrayList<String> couchbaseNodes = new ArrayList<>();
    ArrayList<String> sparkWorkerNodes = new ArrayList<>();
    ArrayList<String> sparkMasterNodes = new ArrayList<>();
    public Bucket bucket = null;
    CouchbaseSparkContext csc = null;
    public JavaSparkContext sc = null;

    public void Initialize() {
        readConfig();
        createSparkContext();
        createBucketConnection();
    }


    void  parseJsonArray(JsonObject jsonObject, String key, ArrayList<String> dest) {
        JsonArray jArray =  jsonObject.get(key).getAsJsonArray();

        for (int i =0; i< jArray.size(); i++) {
            dest.add(i, jArray.get(i).getAsString());
        }

    }

    void readConfig() {
        try {
            String content = new String(Files.readAllBytes(Paths.get("resources/config.json")));
            JsonElement elem = new Gson().fromJson(content, JsonElement.class);
            JsonObject jsonobj = elem.getAsJsonObject();

            parseJsonArray(jsonobj, "couchbase", this.couchbaseNodes);
            parseJsonArray(jsonobj, "spark-master", this.sparkMasterNodes);
            parseJsonArray(jsonobj, "spark-worker", this.sparkWorkerNodes);


        } catch(IOException ex) {
            System.out.println("Unable to parse config file:" + ex.getMessage());
        }
    }

    void createSparkContext() {
        System.out.println(sparkWorkerNodes.get(0));
        SparkConf conf = new SparkConf()
                .setAppName("SparkCbIntegrationTRunner")
                .setMaster("spark://" + sparkMasterNodes.get(0) + ":7077")
                .set("com.couchbase.bucket.default", "")
                .set("com.couchbase.nodes", couchbaseNodes.get(0).toString());


        this.sc = new JavaSparkContext(conf);
        this.csc = couchbaseContext(sc);

        //Add jars so we dont run into class not found exceptions on the worker
        sc.addJar("resources/spark-connector_2.10-1.0.0-beta.jar");
        sc.addJar("resources/java-client-2.1.3.jar");
        sc.addJar("resources/core-io-1.1.3.jar");
        sc.addJar("resources/rxjava-1.0.14.jar");
        sc.addJar("resources/rxscala_2.10-0.23.1.jar");

    }

    void createBucketConnection() {
        CouchbaseEnvironment env = DefaultCouchbaseEnvironment
                .builder()
                .queryEnabled(false)
                .connectTimeout(20)
                .kvTimeout(20)
                .build();

        Cluster cluster = CouchbaseCluster.create(couchbaseNodes.get(0));
        this.bucket = cluster.openBucket();
    }

    void runTests() {
        CouchbaseAdmin admin = new CouchbaseAdmin(this.couchbaseNodes.get(0), "root", "root");
        Tests t = new Tests(this.bucket, this.csc, this.couchbaseNodes.toArray(new String[this.couchbaseNodes.size()]), admin);
        t.TestN1QLRebalanceOut();
    }

    void shutDown() {
        this.sc.stop();
        this.bucket.close();
    }

    public static void main(String[] args) {
        TRUN trun = new TRUN();
        trun.Initialize();
        trun.runTests();
        trun.shutDown();
    }
}
