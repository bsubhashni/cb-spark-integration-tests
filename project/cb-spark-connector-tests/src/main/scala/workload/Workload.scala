package workload

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.{Bucket, PersistTo}
import com.couchbase.spark._
import com.couchbase.spark.connection.CouchbaseBucket
import org.apache.spark.SparkContext

/**
 * Created by subhashni on 10/14/15.
 */

class WorkloadAdmin(wl: Workload) {
  var workloadThr = null

  def startWorkload() {
    wl.initialize()
    new Thread(wl).start()
  }

  def stopWorkload() {
    wl.stop = true
  }

}
 class Workload() extends Runnable {
   val keyPrefix =  "TestKey"
   val valuePrefix = "TestValue"
   val waitTime = 1000
   var stop = false
   var counter = 0

   def run() {
   }
   def initialize(){
     counter = 0
   }
}

class KVWorkload(bucket: Bucket, context: SparkContext) extends Workload {
  override def run() {
    while (!stop) {
      val obj = JsonObject.create()
      obj.put("Value", valuePrefix + counter)
      bucket.upsert(JsonDocument.create(keyPrefix + counter, obj), PersistTo.MASTER)
      counter += 1
      context
        .parallelize(Seq(keyPrefix + counter))
        .couchbaseGet[JsonDocument]()
        .map(_.content())
        .collect();
      Thread sleep(waitTime)
    }
  }

}

class N1QLWorkload(bucket: CouchbaseBucket, context: SparkContext) extends Workload {
  override def run() {
    while (!stop) {

    }
  }
}

class SaveToCBWorkload(bucket: CouchbaseBucket, context: SparkContext) extends Workload {
  override def run() {
    while (!stop) {

    }

  }
}
