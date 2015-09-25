#!/usr/bin/env/python
import os,sys,argparse,json,subprocess

def stop_cluster(prefix):
	args = ["docker-compose", "-p {0}".format(prefix), "kill"]
	p = subprocess.Popen(args)
	p.wait()

parser = argparse.ArgumentParser()
parser.add_argument('--prefix', dest='prefix', required=True, help='Container prefix')
args = parser.parse_args()
stop_cluster(args.prefix)
