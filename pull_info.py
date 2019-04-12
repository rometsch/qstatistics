#!/usr/bin/env python3
import os
import subprocess
from datetime import datetime
import argparse

timestamp =  datetime.now().isoformat()

parser = argparse.ArgumentParser()
parser.add_argument("-d", default=os.getcwd(), help="Output dir. Defaults to cwd")
args = parser.parse_args()
out_dir = args.d

commands = [ ["qstat", "-f"] , ["qstat", "-x"] , ["pbsnodes"], ["pbsnodes", "-x"] ]
suffixes = [ "qstat_string", "qstat_xml", "pbsnodes_string", "pbsnodes_xml" ]
endings = [ ".txt.gz", ".xml.gz", ".txt.gz", ".xml.gz" ]

for cmd, suf, end in zip(commands, suffixes, endings):
	filename = "{}_{}{}".format(suf, timestamp, end)
	filename = os.path.join(out_dir, filename)
	try:
		gzipped_data = subprocess.check_output(["ssh", "binac"] + cmd + ["|", "gzip", "-9"])
	except subprocess.CalledProcessError as e:
		print(e)
		print("{}: failed to get data for {}".format(timestamp, suf))
		continue
	try:
		with open(filename, "bw") as of:
			of.write(gzipped_data)
	except FileNotFoundError:
		print("{}: failed to get data for {}".format(timestamp, suf))
		continue
