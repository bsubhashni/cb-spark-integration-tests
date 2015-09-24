#!/usr/bin/env python
import os
import os.path
import sys

from urlparse import urlparse
from subprocess import Popen, PIPE

URL = sys.argv[1]

# First thing, kill everything.
for pname in ('memcached', 'epmd', 'beam.smp'):
    print "Killing", pname
    po = Popen(('pkill', '-KILL', '-f', pname))
    po.communicate()
sys.stdout.flush()

# Create the cache directory if it does not exist
CACHE_DIR = os.path.expanduser("~/server-packages")
if not os.path.exists(CACHE_DIR):
    os.makedirs(CACHE_DIR)

pr = urlparse(URL)
basepath = os.path.basename(pr[2])  # Py24 doesn't have the NamedTuple version
package = os.path.join(CACHE_DIR, basepath)


EL = False
DEBIAN = False
if os.path.exists('/etc/debian_version'):
    DEBIAN = True
else:
    EL = True


if EL:
    uninst_cmd = ('rpm', '-e', 'couchbase-server')
else:
    uninst_cmd = ('dpkg', '-P', 'couchbase-server')

po = Popen(uninst_cmd, stdout=PIPE, stderr=PIPE)
po.communicate()


if os.path.exists(package):
    st_info = os.stat(package)
    if not st_info.st_size:
        print "File appears to be empty. Removing"
        os.unlink(package)
        sys.stdout.flush()

if not os.path.exists(package):
    print "Package does not exist. Downloading from", URL
    sys.stdout.flush()
    po = Popen(('wget', '-O', package, '--progress=dot:mega', URL))
    po.communicate()
    rv = po.returncode
    if rv != 0:
        print "Could not download!"
        sys.stdout.flush()
        sys.exit(1)

if EL:
    instcmd = ('rpm', '-ivh', package)
else:
    instcmd = ('dpkg', '-i', package)

print "Installing ", package
sys.stdout.flush()
po = Popen(instcmd, stdout=PIPE, stderr=PIPE)
out, err = po.communicate()
if po.returncode != 0:
    print "Installation failed!",
    sys.exit(1)