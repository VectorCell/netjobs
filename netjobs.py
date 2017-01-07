#!/usr/bin/env python3

import sys
import os
import subprocess


class Job:

	jid = 0

	def __init__(self, cmd):
		self.cmd = cmd
		self.id = Job.jid
		Job.jid += 1

	def __repr__(self):
		return 'Job {}: {}'.format(self.id, self.cmd)

	def get_cmd(self):
		return self.cmd


class Host:

	def __init__(self, name):
		self.name = name
		self.busy = False

	def __repr__(self):
		return self.name

	def is_busy(self):
		return self.busy

	def start_job(self, job):
		if self.is_busy():
			raise Exception('ERROR: host is busy')
		self.busy = True
		self.child = subprocess.Popen(['ssh', self.name, job.get_cmd()],
		                              stdout = subprocess.PIPE,
		                              stderr = subprocess.PIPE)
		#print('self.child: {} with pid {}'.format(self.child, self.child.pid))
		return self.child.pid

	def cleanup_job(self):
		out, err = self.child.communicate()
		output = out.decode('utf-8') + err.decode('utf-8')
		self.child = None
		self.busy = False
		return output


class JobDistributor:

	def __init__(self, hosts, jobs):
		self.hosts = tuple(h for h in hosts if isinstance(h, Host))
		self.jobs = tuple(j for j in jobs if isinstance(j, Job))
		self.job_next = 0
		self.n_running = 0
		self.pid_job_map = {}
		self.job_host_map = {}

	def __repr__(self):
		val = ''
		for h in self.hosts:
			val += 'host: {}\n'.format(h)
		for j in self.jobs:
			val += 'job:  {}\n'.format(j)
		return val.strip()

	def __has_next_job(self):
		return self.job_next < len(self.jobs)

	def __get_next_job(self):
		if not self.__has_next_job():
			return None
		ret = self.jobs[self.job_next]
		self.job_next += 1
		#print('next job:', ret)
		return ret

	def __get_available_host(self):
		for host in self.hosts:
			if not host.is_busy():
				return host
		return None

	def __start_job(self, host, job):
		pid = host.start_job(job)
		self.pid_job_map[pid] = job
		self.job_host_map[job] = host
		#print('started job {} with pid {} on host {}'.format(job.id, pid, host))

	def __start_next_job(self):
		if self.__has_next_job():
			host = self.__get_available_host()
			job = self.__get_next_job()
			self.__start_job(host, job)

	def __cleanup_job(self, pid):
		job = self.pid_job_map.pop(pid, None)
		host = self.job_host_map.pop(job, None)
		output = host.cleanup_job()
		print(output.strip())

	def start(self):
		for host in self.hosts:
			self.__start_job(host, self.__get_next_job())
		while True:
			try:
				pid, status = os.wait()
				#print('wait returned pid {}'.format(pid))
				self.__cleanup_job(pid)
				self.__start_next_job()
			except ChildProcessError as cpe:
				break


def run_jobs_from_lists(hostsfilename, jobsfilename):
	with open(hostsfilename) as hostsfile:
		hosts = [Host(line.strip()) for line in hostsfile if line]
	with open(jobsfilename) as jobsfile:
		jobs = [Job(line.strip()) for line in jobsfile if line]
	jd = JobDistributor(hosts, jobs)
	jd.start()


def convert_video_files(directory):
	print('dir:', directory)
	videofiles = []
	for subdir, dirs, files in os.walk(directory):
		for file in files:
			path = os.path.join(subdir, file)
			print(path)
			if path[::-1].startswith('.avi'[::-1]):
				videofiles.append(path)
	jobs = []
	for src in videofiles:
		dest = src.replace('.avi', '.mkv')
		lines = []
		lines.append('ffmpeg -i "{}" -c:v libx265 -c:a copy "{}"'.format(src, dest))
		lines.append('rm "{}"'.format(src))
		jobs.append(' && '.join(lines))
	for job in jobs:
		print(job)


def main(argv):
	if len(argv) >= 2:
		convert_video_files(argv[1])
	else:
		run_jobs_from_lists('hosts.txt', 'jobs.txt')


if __name__ == '__main__':
	main(sys.argv)
