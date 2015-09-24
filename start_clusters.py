#!/usr/bin/env/python
import os,sys,argparse,json,subprocess,paramiko

spark_worker_container='spark_worker'
spark_master_container='spark_master'
couchbase_container='couchbase_base'
couchbase_ips = []
spark_worker_ips = []
spark_master_ips = []
container_prefix = "cbspark"

def run_command(args):
	p = subprocess.Popen(args)
	p.wait()
	if p.returncode != 0:
		print '{0} failed with exit status'.format(p.returncode)
		os._exit(1)

def get_ips(cb_nodes, spark_workers):
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

	with open("config.ini", "w+") as f:
		f.write("[global]\n")
		f.write("username:root\n")
		f.write("password:couchbase\n")
		f.write("[servers]\n")
		f.write("port:8091\n")
		for i  in range(1, int(cb_nodes) + 1):
			f.write("{0}:{1}\n".format(i, couchbase_ips[i-1]))
		f.write("[membase]\n")
		f.write("rest_username:Administrator\n")
		f.write("rest_username:password\n")

def setup_couchbase_cluster(version):
	args = ["git", "clone", "https://github.com/couchbase/testrunner.git" ]
	run_command(args)
	args = ["python" , "scripts/install.py", "-p", "product=cb,version={0}".format(version)]
	run_command(args)

def start_environment(cbnodes, sparkworkers):
	cb_args = "couchbase_base={0}".format(cbnodes)
	spark_worker_args = "spark_worker={0}".format(sparkworkers)
	args = ["docker-compose", "-p={0}".format(container_prefix), "scale", cb_args, spark_worker_args, "spark_master=1"]
	run_command(args)

def cleanup_environment():
	args = ["python", "stop_cluster.py"]
	run_command(args)


parser = argparse.ArgumentParser(description='Setup couchbase and spark clusters. Currently supports one spark master')
parser.add_argument('--couchbase-nodes', dest='cbnodes', required=True, help='Number of couchbase nodes in cb cluster')
parser.add_argument('--spark-workers', dest='sparkworkers', required=True, help='Number of spark workers in spark cluster')
parser.add_argument('--cb-version', dest='cbversion', default='4.0.0.-4057', help='Couchbase-server version')

args = parser.parse_args()
cleanup_environment()
start_environment(args.cbnodes, args.sparkworkers)
get_ips(args.cbnodes, args.sparkworkers)
#setup_couchbase_cluster(args.cbversion)
