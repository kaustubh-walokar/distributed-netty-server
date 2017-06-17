#!/bin/bash
#
# creates the python classes for our .proto
#

#project_base="/Users/gash/workspace/messaging/core-netty/python"
project_base="/Users/kaustubh/275/proj2/core-netty-4.4/python"
rm ${project_base}/src/comm_pb2.py

protoc -I=${project_base}/resources --python_out=./src ${project_base}/resources/comm.proto
