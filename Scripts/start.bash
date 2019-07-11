#!/bin/bash

git pull
sbt runMain adaptivecep.distributed.HostSystem node

