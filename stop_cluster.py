#!/usr/bin/env/python
import os,sys,argparse,json,subprocess

def stop_cluster():
	args = ["docker-compose", "kill"]	
	p = subprocess.Popen(args)
	p.wait()

stop_cluster()
