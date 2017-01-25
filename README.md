# cb-spark-integration
* Dependencies:
	* docker
	* docker-compose
	* python modules
		* paramiko
		* scp

* Configuration  
	To start the containerized couchbase and spark cluster environment run 
	python start_clusters.py --cb-nodes=2 --spark-workers=2 --url=<download url for couchbase build> 
	--prefix cbspark

	To stop them use python stop_clusters.py --prefix cbspark

	This currently with only one spark master

