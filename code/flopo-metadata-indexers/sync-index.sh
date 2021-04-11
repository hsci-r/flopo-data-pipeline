#!/bin/bash
POD=`oc -n filter get pod -l deploymentconfig==flopo-octavo -o name`
oc -n filter rsync --delete=true $1 $POD:/opt/docker/index/$2
