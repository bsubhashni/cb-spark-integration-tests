#!/usr/bin/env/python
import os,sys,argparse,json,subprocess,paramiko,requests,time,threading
from scp import SCPClient

spark_worker_container='spark_worker'
spark_master_container='spark_master'
couchbase_container='couchbase_base'
couchbase_ips = []
spark_worker_ips = []
spark_master_ips = []
container_prefix = "cbspark"
cluster_config_file = "config.json"
buckets = ['default']
masterIp = None
masterClient = None

def run_command(args):
	p = subprocess.Popen(args)
	p.wait()
	if p.returncode != 0:
		print '{0} failed with exit status'.format(p.returncode)
		os._exit(1)

def get_ips_and_configure(cb_nodes, spark_workers, download_url):
	for i in range(1, int(cb_nodes) + 1):
		container_id = "{0}_{1}_{2}".format(container_prefix, couchbase_container, i)
		args = ["docker", "inspect", "--format='{{.NetworkSettings.IPAddress}}'", container_id]
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
		couchbase_ips.append(out.rstrip())

	for i in range(1, int(spark_workers) + 1):
		container_id = "{0}_{1}_{2}".format(container_prefix, spark_worker_container, i)
		args = ["docker", "inspect", "--format='{{.NetworkSettings.IPAddress}}'", container_id]
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
		spark_worker_ips.append(out.rstrip())

	for i in range(1, 2):
		container_id = "{0}_{1}_{2}".format(container_prefix, spark_master_container, i)
		args = ["docker", "inspect", "--format='{{.NetworkSettings.IPAddress}}'", container_id]
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
		spark_master_ips.append(out.rstrip())

	cluster_config = json.dumps({"couchbase" : couchbase_ips, "spark-worker" : spark_worker_ips, "spark-master" : spark_master_ips })

	with open(cluster_config_file, "w+") as f:
		f.write(cluster_config)

	tasks = []

	lock = threading.Lock()
	for i in range(0, int(cb_nodes)):
		isMaster = False
		if i == 0:
			isMaster = True
		task = threading.Thread(target=install_couchbase, args=(couchbase_ips[i],download_url, isMaster))
		task.start()
		tasks.append(task)
	[task.join() for task in tasks]

	time.sleep(20) #install should not take longer than this - use a better way
	for i in range(0, int(cb_nodes)):
		r = requests.get("http://{0}:8091/pools".format(couchbase_ips[i]))
		if r.status_code != 200:
			print "Server not installed correctly. Received status code:".format(r.status_code)
			os._exit(1)

	initialize_nodes_rebalance(couchbase_ips)

def install_couchbase(ip, url, isMaster):
	client = paramiko.SSHClient()
	client.load_system_host_keys()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	client.connect(ip, username="root", password="root")
	scp = SCPClient(client.get_transport())
	if isMaster == True:
		global masterClient
		masterClient = client
		global masterIp
		masterIp = ip
	scp = SCPClient(client.get_transport())
	scp.put('cluster-install.py', 'cluster-install.py')
	command = "python cluster-install.py {0}".format(url)
	(stdin, stdout, stderr) = client.exec_command(command)
	for line in stdout.readlines():
		print line
	if isMaster != True:
		client.close()

def initialize_nodes_rebalance(nodes):
	print masterClient
	command = ("/opt/couchbase/bin/couchbase-cli cluster-init -c 127.0.0.1:8091 "
				   "--cluster-init-username=Administrator --cluster-init-password=password --cluster-init-ramsize=512 "
				   "--services=data,index,query")
	(stdin, stdout, stderr) = masterClient.exec_command(command)
	for line in stdout.readlines():
		print line

	command = ("/opt/couchbase/bin/couchbase-cli bucket-create -c 127.0.0.1:8091 "
					"-u Administrator -p password --bucket=default -c localhost:8091 --bucket-ramsize=512")
	(stdin, stdout, stderr) = masterClient.exec_command(command)
	for line in stdout.readlines():
		print line

	if len(nodes) > 1:
		server_add = ''
		for i in range(1, len(nodes)):
			print i
			print nodes[i]
			server_add += (" --server-add={0}:8091 --server-add-username=Administrator --server-add-password=password"
						   " --services=data,index,query".format(nodes[i]))

		command = ("/opt/couchbase/bin/couchbase-cli rebalance -c 127.0.0.1:8091 {0} -u Administrator -p password"
					" ".format(server_add))
		print command
		(stdin, stdout, stderr) = masterClient.exec_command(command)
		for line in stdout.readlines():
			print line

def start_environment(cbnodes, sparkworkers):
	cb_args = "couchbase_base={0}".format(cbnodes)
	spark_worker_args = "spark_worker={0}".format(sparkworkers)
	args = ["docker-compose", "-p={0}".format(container_prefix), "scale", cb_args,  "spark_master=1",  spark_worker_args]
	run_command(args)

def cleanup_environment():
	args = ["python", "stop_cluster.py", "--prefix={0}".format(container_prefix)]
	run_command(args)

parser = argparse.ArgumentParser(description='Setup couchbase and spark clusters. Currently supports one spark master')
parser.add_argument('--cb-nodes', dest='cbnodes', required=True, help='Number of couchbase nodes in cb cluster')
parser.add_argument('--spark-workers', dest='sparkworkers', required=True, help='Number of spark workers in spark cluster')
parser.add_argument('--url', dest='url', required=True, help='Couchbase-server version')
args = parser.parse_args()
cleanup_environment()
start_environment(args.cbnodes, args.sparkworkers)
get_ips_and_configure(args.cbnodes, args.sparkworkers, args.url)
