#!/bin/bash

PYTHON=$(which python)
PIP=$(which pip)

if [ ! -x "${PYTHON}" ]; then
    echo "Python doesn't exist or cannot be executed"
    exit
fi

python -c $'import sys\nif sys.version_info < (3, 0): sys.exit(1);'

if [ $? -ne 0 ]; then
    #
    # Let's see if python3 exists
    #
    PYTHON=$(which python3)
    if [ ! -x "${PYTHON}" ]; then
        echo "Python version 3 or above needed."
        exit
    else
        PIP=$(which pip3) # We use pip3 if python3 exists
    fi
fi

if [ ! -x "${PIP}" ]; then
    echo "Pip doesn't exist or cannot be executed"
    exit
fi

echo "Installing requirements...."
${PIP} install -r requirements.txt
echo "Processing inputs and generating results..."
${PYTHON} solution.py
echo "Results generated in results.txt"