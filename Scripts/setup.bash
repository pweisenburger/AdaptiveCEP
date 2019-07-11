#!/bin/bash

SBT_OPTS="-Xmx512M"
GLOBALIP=$(curl ipecho.net/plain)
LOCALIP=$(hostname -I)
cd AdaptiveCEP
