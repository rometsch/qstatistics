#!/usr/bin/env python
import subprocess
import argparse

def argsort(seq):
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)

ap = argparse.ArgumentParser()

ap.add_argument("hostname", nargs="?", help="Specify the remote user@hostname which is used to login to the remote machine via ssh.")
args = ap.parse_args()

# First run qstat on the local host if its supported.
try:
	# Look for qstat
	qstat_location = subprocess.check_output(["which", "qstat"])
	qstat_cmd = ["qstat"]
except subprocess.CalledProcessError:
	if not args.hostname is None:
		qstat_cmd = ["ssh", args.hostname, "qstat"]
	else:
		print("qstat not found on the system and no host specified! Exit...")
		ap.print_help()
		exit(1)

# Now get the queue info
queue_str = subprocess.check_output(qstat_cmd + ["-f"]).decode("ascii")
jobs = {}
job = {}
key = ""
value = ""
for line in queue_str.splitlines():
    try:
        # Check if the line starts a new block
        if line[:7] == "Job Id:":
            jobid = line.split(":")[1].lstrip()
            job = {"jobid" : jobid}
            jobs[jobid] = job
        # Check if the lines starts a usual key value pair
        if line[:4] == "    ":
            parts = line.lstrip().split("=")
            key = parts[0].strip()
            if len(parts) > 1:
                value = "=".join(parts[1:]).strip()
            else:
                value = ""
            job[key] = value
        # Check if the line continues an entry
        if line[0] == "\t":
            value = line.lstrip("\t").strip()
            job[key] += value
    except (IndexError, KeyError):
        pass

 # Make a dict of users and the number of jobs they have, also log queue
user_stats = {}
for key in jobs:
    user = jobs[key]["euser"]
    queue = jobs[key]["queue"]
    if not user in user_stats:
        user_stats[user] = { "num_gpu_jobs" : 0, "num_cpu_jobs" : 0 }
    if queue in ["gpu", "tiny"]:
        user_stats[user]["num_gpu_jobs"] += 1
    else:
        user_stats[user]["num_cpu_jobs"] += 1

# Print the users with the most jobs
NCpuJobs = []
NGpuJobs = []
Users = []
for user in user_stats:
    Users.append(user)
    NCpuJobs.append(user_stats[user]["num_cpu_jobs"])
    NGpuJobs.append(user_stats[user]["num_gpu_jobs"])

NToList = 10

NJobs = NCpuJobs
inds = argsort(NJobs)
inds.reverse()
print("Users with most CPU jobs")
N = min(NToList, len(NJobs))
for n in inds[:N]:
	print("{} : {}".format(Users[n], NJobs[n]))


NJobs = NGpuJobs
inds = argsort(NJobs)
inds.reverse()
print("\nUsers with most GPU jobs")
N = min(NToList, len(NJobs))
for n in inds[:N]:
	print("{} : {}".format(Users[n], NJobs[n]))