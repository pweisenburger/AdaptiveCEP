#!/bin/bash

CURRENTIP=$1
ssh -i "AdaptiveCEP.pem" -o ServerAliveInterval=30 ec2-user@$CURRENTIP
