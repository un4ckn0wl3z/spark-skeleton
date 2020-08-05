#!/usr/bin/env bash

# check to see if pipenv is installed
if [ -x "$(which pipenv)" ]
then
    # check that Pipfile.lock exists in root directory
    if [ ! -e Pipfile.lock ]
    then
        echo 'ERROR - cannot find Pipfile.lock'
        exit 1
    fi

    # use Pipenv to create a requirement.txt file
    echo '... creating requirements.txt from Pipfile.lock'
    pipenv lock -r > requirements.txt
    pip3 install -r requirements.txt -t ./libs
    exit 0
else
    echo 'ERROR - pipenv is not installed --> run `pip3 install pipenv` to load pipenv into global site packages or install via a system package manager.'
    exit 1
fi
