# netjobs


This is a tool for distributing and coordinating jobs on multiple machines over a network.

Currently, each available host must be on a separate line of `hosts.txt`, and each job must be on a separate line of `jobs.txt`. If each job consists of multiple commands, they must still be on the same line, separated by a semicolon.

I originally created this to facilitate using multiple computers to work on encoding several video files at the same time using [ffmpeg](https://ffmpeg.org/).
