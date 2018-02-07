#!/usr/bin/env python
import subprocess
import argparse

def argsort(seq):
    # http://stackoverflow.com/questions/3071415/efficient-method-to-calculate-the-rank-vector-of-a-list-in-python
    return sorted(range(len(seq)), key=seq.__getitem__)

ap = argparse.ArgumentParser(usage="Print the users with the highest number of jobs in the queue.\nSearch the local PBS queue by default or connect to the host given as first argument." )

ap.add_argument("hostname", nargs="?",
				help="Specify the remote host's user@hostname which is used to login to the remote machine via ssh.")
ap.add_argument("-N", "--NumToPrint", default=10, type=int,
				help="Print the N users with the highest job count")
ap.add_argument("-q", "--queue", nargs="+",
				default=None,
				help="Queues to print the statistics for. Print all found queues by default. Add queues separeted by a space.")
ap.add_argument("-f", help="Use a text file as input rather than using the qstat -f command. This is useful for development")
ap.add_argument("-s", "--sort", choices = ["t", "r", "q"], default = "r"
				, help="The number to sort the output by:\nt -> total number of jobs\nr -> running jobs\nq -> queued jobs")
args = ap.parse_args()

# First run qstat on the local host if its supported.
if not args.hostname is None:
   	qstat_cmd = ["ssh", args.hostname, "qstat"]
elif args.f is None:
	try:
		# Look for qstat
		qstat_location = subprocess.check_output(["which", "qstat"])
		qstat_cmd = ["qstat"]
	except subprocess.CalledProcessError:
		print("qstat not found on the system and no host specified! Exit...")
		ap.print_help()
		exit(1)

if args.f is None:
	# Now get the queue info
	queue_str = subprocess.check_output(qstat_cmd + ["-f"]).decode("ascii")
else:
	with open(args.f, "r") as f:
		queue_str = f.read()
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
found_queues = []
for key in jobs:
	user = jobs[key]["euser"]
	queue = jobs[key]["queue"]
	state = jobs[key]["job_state"]
	if not queue in found_queues:
		found_queues.append(queue)
	if not user in user_stats:
		user_stats[user] = {}
	if not queue in user_stats[user]:
		user_stats[user][queue] = { state : 1, "tot" : 1 }
	else:
		user_stats[user][queue]["tot"] += 1
		if not state in user_stats[user][queue]:
			user_stats[user][queue][state] = 1
		else:
			user_stats[user][queue][state] += 1


# Sort the queue names
found_queues.sort()

# Select the queues
if not args.queue is None:
	selected_queues = args.queue
else:
	selected_queues = found_queues

# Make lists for each queue with job count and user names
queue_stats = {}
for queue in selected_queues:
	queue_stats[queue] = {"users" : [], "jobstot" : [],
						  "total" : 0, "jobsRunning" : [], "jobsQueued" : []}
for user in user_stats:
	for queue in selected_queues:
		if queue in user_stats[user]:
			queue_stats[queue]["users"].append(user)
			queue_stats[queue]["jobstot"].append(user_stats[user][queue]["tot"])
			queue_stats[queue]["total"] += user_stats[user][queue]["tot"]
			for state, key in zip(["R","Q"], ["jobsRunning", "jobsQueued"]):
				if state in user_stats[user][queue]:
					num = user_stats[user][queue][state]
				else:
					num = 0
				queue_stats[queue][key].append(num)


NToList = args.NumToPrint

len_user_string = 15
len_cnt_string = 6

def fixed_length_string(s, length):
	template = "{" + ":{}s".format(length) + "}"
	return template.format(s)

for queue in selected_queues:
	jobcnt = ["total", "-"*5] + queue_stats[queue]["jobstot"]
	runcnt = ["run", "-"*3] + queue_stats[queue]["jobsRunning"]
	quecnt = ["queued", "-"*6] + queue_stats[queue]["jobsQueued"]
	users = ["# user","#"+ "-"*(len_user_string-1)] + queue_stats[queue]["users"]
	if args.sort == "t":
		sort_list = jobcnt
	elif args.sort == "q":
		sort_list = quecnt
	else:
		sort_list = runcnt
	inds = argsort(sort_list)
	inds.reverse()
	print("\n#-----------------------------------")
	print("# queue:\t{}".format(queue))
	print("# jobs: \t{}".format(queue_stats[queue]["total"]))
	print("#-----------------------------------")
	N = min(NToList, len(jobcnt))
	for n in inds[:N]:
		user_string = fixed_length_string(users[n], len_user_string)
		tot_cnt_string = fixed_length_string("{}".format(jobcnt[n]), len_cnt_string)
		run_cnt_string = fixed_length_string("{}".format(runcnt[n]), len_cnt_string)
		que_cnt_string = fixed_length_string("{}".format(quecnt[n]), len_cnt_string)
		print("{}   {} {} {}".format(user_string,run_cnt_string, que_cnt_string, tot_cnt_string ))
