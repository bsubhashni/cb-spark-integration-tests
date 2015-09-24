#!/usr/bin/env/python
import os,sys,argparse,json,subprocess, paramiko
from scp import SCPClient
import threading

spark_worker_container='spark_worker'
spark_master_container='spark_master'
couchbase_container='couchbase_base'
couchbase_ips = []
spark_worker_ips = []
spark_master_ips = []
container_prefix = "cbspark"
install_config_file = "config.json"
buckets = ['default']

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
		print args
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
		couchbase_ips.append(out.rstrip())

	for i in range(1, int(spark_workers) + 1):
		container_id = "{0}_{1}_{2}".format(container_prefix, spark_worker_container, i)
		args = ["docker", "inspect", "--format='{{.NetworkSettings.IPAddress}}'", container_id]
		print args
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
		spark_worker_ips.append(out.rstrip())

	for i in range(1, 2):
		container_id = "{0}_{1}_{2}".format(container_prefix, spark_master_container, i)
		args = ["docker", "inspect", "--format='{{.NetworkSettings.IPAddress}}'", container_id]
		print args
		process = subprocess.Popen(args, stdout=subprocess.PIPE)
		out, err = process.communicate()
		spark_master_ips.append(out.rstrip())

	with open(install_config_file, "w+") as f:
		f.write("{\n")
		f.write("\"couchbase\":{0},\n".format(couchbase_ips))
		f.write("\"spark-master\":{0},\n".format(spark_worker_ips))
		f.write("\"spark-worker\":{0}\n".format(spark_master_ips))
		f.write("}")

	tasks = []
	for i in range(0, int(cb_nodes)):
		task = threading.Thread(target=install_couchbase, args=(couchbase_ips[i],download_url,))
		task.start()
		tasks.append(task)

	[task.join() for task in tasks]

def install_couchbase(ip, url):
	client = paramiko.SSHClient()
	client.load_system_host_keys()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	client.connect(ip, username="root", password="root")
	scp = SCPClient(client.get_transport())
	scp.put('cluster-install.py', 'cluster-install.py')
	command = "python cluster-install.py {0}".format(url)
	print command
	(stdin, stdout, stderr) = client.exec_command(command)
	for line in stdout.readlines():
		print line
	client.close()

def start_environment(cbnodes, sparkworkers):
	cb_args = "couchbase_base={0}".format(cbnodes)
	spark_worker_args = "spark_worker={0}".format(sparkworkers)
	args = ["docker-compose", "-p={0}".format(container_prefix), "scale", cb_args, spark_worker_args, "spark_master=1"]
	run_command(args)

def cleanup_environment():
	args = ["python", "stop_cluster.py"]
	run_command(args)

parser = argparse.ArgumentParser(description='Setup couchbase and spark clusters. Currently supports one spark master')
parser.add_argument('--cb-nodes', dest='cbnodes', required=True, help='Number of couchbase nodes in cb cluster')
parser.add_argument('--spark-workers', dest='sparkworkers', required=True, help='Number of spark workers in spark cluster')
parser.add_argument('--url', dest='url', required=True, help='Couchbase-server version')
args = parser.parse_args()
cleanup_environment()
start_environment(args.cbnodes, args.sparkworkers)
get_ips_and_configure(args.cbnodes, args.sparkworkers, args.url)
