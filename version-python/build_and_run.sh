#!/bin/bash

SCRIPT=$(readlink -f "$0")
BASEDIR=$(dirname "$SCRIPT")
cd $BASEDIR

python3 CodeCraft-2022/src/CodeCraft-2022.py
