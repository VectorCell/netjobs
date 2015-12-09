#pragma once

#include <string>
#include <fstream>
#include <sstream>

struct job_t;
struct host_t;
typedef struct job_t job_t;
typedef struct host_t host_t;

struct job_t {
	pid_t pid;
	std::string cmd;
	bool finished;
	int status;
	host_t *host;
};

struct host_t {
	std::string name;
	job_t *job;
};

template <class C>
bool read_file(const std::string& filename, C& destination)
{
	using namespace std;
	const string comment_prefix = "#";
	string line;
	ifstream reader;
	reader.open(filename);
	if (reader.is_open()) {
		while (getline(reader, line)) {
			if ((line.size() != 0) && !equal(comment_prefix.begin(), comment_prefix.end(), line.begin())) {
				istringstream ss(line);
				destination.push_back(line);
			}
		}
		reader.close();
		return true;
	} else {
		return false;
	}
}
