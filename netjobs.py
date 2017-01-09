#!/usr/bin/env python3

import sys
import os
import subprocess


CMD_SETUP = '''true'''
CMD_CLEANUP = '''
	echo hello world from $HOSTNAME
	echo this truly is $HOSTNAME
'''


class Job:

	jid = 1

	def __init__(self, cmd):
		self.cmd = cmd
		self.id = Job.jid
		Job.jid += 1

	def __repr__(self):
		return 'Job {}/{}: {}'.format(self.id, Job.get_num_jobs(), self.cmd)

	def get_cmd(self):
		return self.cmd

	@staticmethod
	def get_num_jobs():
		return Job.jid - 1


class Host:

	def __init__(self, name):
		self.name = name
		self.busy = False
		self.n_jobs = 0

	def __repr__(self):
		return self.name

	def __format_output(self, output):
		lines = output.split('\n')
		after = ''
		for line in lines:
			if line:
				after += '\033[96m{}\033[0m :: {}\n'.format(self.name, line)
		after = after.strip()
		return after

	def is_busy(self):
		return self.busy

	def run_cmd(self, cmd):
		if cmd:
			p = subprocess.Popen(['ssh', self.name, cmd],
			                     stdout = subprocess.PIPE,
			                     stderr = subprocess.PIPE)
			out, err = p.communicate()
			if p.returncode != 0:
				raise ChildProcessError('{} bad cmd: {}'.format(self.name, cmd))
			output = out.decode('utf-8')
			return self.__format_output(output)

	def start_job(self, job):
		if job and job.get_cmd():
			if self.is_busy():
				raise Exception('ERROR: host is busy')
			self.busy = True
			self.child = subprocess.Popen(['ssh', self.name, job.get_cmd()],
			                              stdout = subprocess.PIPE,
			                              stderr = subprocess.PIPE)
			self.n_jobs += 1
			return self.child.pid
		return -1

	def cleanup_job(self):
		out, err = self.child.communicate()
		output = out.decode('utf-8') + err.decode('utf-8')
		self.child = None
		self.busy = False
		return self.__format_output(output)

	def get_n_jobs(self):
		return self.n_jobs


class JobDistributor:

	def __init__(self, hosts, jobs, verbose = True, showremote = False):
		self.hosts = tuple(h for h in hosts if isinstance(h, Host))
		self.jobs = tuple(j for j in jobs if isinstance(j, Job))
		self.job_next = 0
		self.n_running = 0
		self.map_pid_job = {}
		self.map_job_host = {}
		self.verbose = verbose
		self.showremote = showremote

	def __repr__(self):
		val = ''
		for h in self.hosts:
			val += 'host: {}\n'.format(h)
		for j in self.jobs:
			val += 'job:  {}\n'.format(j)
		return val.strip()

	def __setup_host(self, host):
		return host.run_cmd(CMD_SETUP)

	def __cleanup_host(self, host):
		return host.run_cmd(CMD_CLEANUP)

	def __has_next_job(self):
		return self.job_next < len(self.jobs)

	def __get_next_job(self):
		if not self.__has_next_job():
			return None
		ret = self.jobs[self.job_next]
		self.job_next += 1
		return ret

	def __get_available_host(self):
		for host in self.hosts:
			if not host.is_busy():
				return host
		return None

	def __start_job(self, host, job):
		if host and job:
			pid = host.start_job(job)
			self.map_pid_job[pid] = job
			self.map_job_host[job] = host
			if self.verbose:
				print('started job {}/{} on host {}'.format(job.id, Job.get_num_jobs(), host))

	def __start_next_job(self):
		if self.__has_next_job():
			host = self.__get_available_host()
			job = self.__get_next_job()
			self.__start_job(host, job)

	def __cleanup_job(self, pid):
		job = self.map_pid_job.pop(pid, None)
		host = self.map_job_host.pop(job, None)
		return host.cleanup_job()

	def run(self):
		for host in self.hosts:
			output = self.__setup_host(host)
			if self.showremote and output:
				print(output)
		for host in self.hosts:
			self.__start_job(host, self.__get_next_job())
		while True:
			try:
				pid, status = os.wait()
				output = self.__cleanup_job(pid)
				if self.showremote and output:
					print(output)
				self.__start_next_job()
			except ChildProcessError as cpe:
				break
		for host in self.hosts:
			output = self.__cleanup_host(host)
			if self.showremote and output:
				print(output)
		if self.verbose:
			total_jobs = 0
			for host in self.hosts:
				total_jobs += host.get_n_jobs()
				print('{} ran {} jobs'.format(host, host.get_n_jobs()))
			print('total jobs run: {}'.format(total_jobs))


def run_jobs_from_lists(hostsfilename, jobsfilename):
	with open(hostsfilename) as hostsfile:
		hosts = [Host(line.strip()) for line in hostsfile if line]
	with open(jobsfilename) as jobsfile:
		jobs = [Job(line.strip()) for line in jobsfile if line]
	jd = JobDistributor(hosts, jobs, showremote = False)
	return jd


def convert_video_files(directory):
	print('dir:', directory)
	videofiles = []
	for subdir, dirs, files in os.walk(directory):
		for file in files:
			path = os.path.join(subdir, file)
			if path[::-1].startswith('.avi'[::-1]):
				videofiles.append(path)

	with open('hosts.txt') as hostsfile:
		hosts = [Host(line.strip()) for line in hostsfile if line]

	jobs = []
	for src in videofiles:
		dest = src.replace('.avi', '.mkv')
		lines = []
		#lines.append('ls -l "{}"'.format(src))
		lines.append('ffmpeg -i "{}" -c:v libx265 -c:a copy "{}" &> ~/x'.format(src, dest))
		#lines.append('rm "{}"'.format(src))
		jobs.append(Job(' && '.join(lines)))
	jd = JobDistributor(hosts, jobs)
	return jd


def main(argv):
	if len(argv) >= 2:
		jd = convert_video_files(argv[1])
	else:
		jd = run_jobs_from_lists('hosts.txt', 'jobs.txt')
	try:
		jd.run()
	except ChildProcessError as cpe:
		print(cpe)


if __name__ == '__main__':
	main(sys.argv)
