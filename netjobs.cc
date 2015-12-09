#include <iostream>

#include <string>
#include <vector>

#include <unistd.h>   // fork, exec
#include <sys/wait.h> // waitpid
#include <csignal>

#include "netjobs.h"

using namespace std;

bool has_job(const host_t& host)
{
	return host.job != NULL;
}

pid_t start_job(host_t& host)
{
	if (has_job(host)) {
		pid_t pid = fork();
		if (pid == 0) {
			char buf[128];
			sprintf(buf, "ssh %s '%s'", host.name.c_str(), host.job->cmd.c_str());
			//printf("running job: %s\n", buf);
			int retval = system(buf);
			exit(retval);
		} else {
			host.job->pid = pid;
			host.job->host = &host;
			return pid;
		}
	} else {
		printf("ERROR: host %s does not have a job to start\n", host.name.c_str());
		return -1;
	}
}

bool cleanup_job(pid_t pid, int status, vector<host_t>& hosts)
{
	for (unsigned k = 0; k < hosts.size(); ++k) {
		if (has_job(hosts[k]) && hosts[k].job->pid == pid) {
			hosts[k].job->finished = true;
			hosts[k].job->status = status;
			hosts[k].job = NULL;
			return true;
		}
	}
	return false;
}

int get_free_host_index(vector<host_t>& hosts)
{
	for (unsigned k = 0; k < hosts.size(); ++k) {
		if (!has_job(hosts[k])) {
			return k;
		}
	}
	return -1;
}

pid_t assign_to_host(job_t& job, host_t& host)
{
	if (!has_job(host)) {
		//printf("giving job [%s] to host %s\n", job.cmd.c_str(), host.name.c_str());
		host.job = &job;
		return start_job(host);
	} else {
		return -1;
	}
}

pid_t assign_next(job_t& job, vector<host_t>& hosts)
{
	for (unsigned k = 0; k < hosts.size(); ++k) {
		if (!has_job(hosts[k])) {
			return assign_to_host(job, hosts[k]);
		}
	}
	printf("ERROR: all hosts have jobs\n");
	return -1;
}

bool init(vector<job_t>& jobs, vector<host_t>& hosts)
{
	vector<string> cmds;
	if (!read_file("jobs.txt", cmds)) {
		return false;
	}
	for (string& cmd : cmds) {
		job_t job;
		job.pid = -1;
		job.cmd = cmd;
		job.finished = false;
		job.status = -1;
		jobs.push_back(job);
		//printf("adding job [%s]\n", jobs[jobs.size() - 1].cmd.c_str());
	}

	vector<string> hostnames;
	if (!read_file("hosts.txt", hostnames)) {
		return false;
	}
	for (string& hostname : hostnames) {
		host_t host;
		host.name = hostname;
		host.job = NULL;
		hosts.push_back(host);
		//printf("adding host %s\n", hostname.c_str());
	}

	return true;
}

int main(int argc, char *argv[])
{
	vector<job_t> jobs;
	vector<host_t> hosts;
	if (!init(jobs, hosts))
		return 1;

	unsigned n_assigned_jobs = 0;

	// initially fill all hosts with jobs
	for (unsigned k = 0; k < min(hosts.size(), jobs.size()); ++k) {
		pid_t pid = assign_to_host(jobs[k], hosts[k]);
		if (pid == -1)
			printf("ERROR: unable to assign_to_host\n");
		else
			++n_assigned_jobs;
	}

	// assign jobs as others finish
	pid_t pid;
	int status;
	for (unsigned k = 0; k < jobs.size(); ++k) {
		while ((pid = waitpid(-1, &status, 0)) > 0) {
			if (WIFEXITED(status)) {

				cleanup_job(pid, WEXITSTATUS(status), hosts);
				if (n_assigned_jobs < jobs.size())
				{
					//printf("assigning next job ...\n");
					if (assign_next(jobs[n_assigned_jobs], hosts) != -1)
						++n_assigned_jobs;
					else
						printf("ERROR: couldn't assign next job\n");
				}

				// reap
				if (WEXITSTATUS(status) != 0)
					printf("ERROR: child %d exited with error status %d\n", pid, WEXITSTATUS(status));
			} else if (WIFSIGNALED(status)) {
				if (WTERMSIG(status) == SIGINT) {
					// killed with sigint
				}
			} else if (WIFSTOPPED(status)) {
				if (WSTOPSIG(status) == SIGTSTP) {
					// terminal stop
				}
			}
		}
	}

	for (job_t& job : jobs) {
		printf("job [%s] ran on %s, exited with status %d\n", job.cmd.c_str(), job.host->name.c_str(), job.status);
	}

	return 0;
}
