# Celeborn CLI User Guide

## Build
- Create the distribution:
```sh
./build/make-distribution.sh
```

- Inside the distribution, there should be a new directory called cli-jars. This directory contains all the jars necessary for the Celeborn CLI to work. 
  - Please note that Celeborn CLI requires version 0.6+ of Celeborn to work since it depends on OpenAPI for API calls.

## Setup
- Set JAVA_HOME if not set already on your machine: 
```sh
export JAVA_HOME=/path/to/java/home
```
- Add the path of the sbin dir to your $PATH:
```sh
export PATH=$PATH:/path/to/CELEBORN_HOME/apache-celeborn-0.6.0-SNAPSHOT-bin/sbin
```
- Verify celeborn-cli works:
```sh
[~]$ celeborn-cli --version
Celeborn CLI - Celeborn 0.6.0-SNAPSHOT
[~]$
```

## Using Celeborn CLI
The commands available can be seen via the help command. For example, for master commands:
```sh
celeborn-cli master -h
```
Similarily, for worker commands:
```sh
celeborn-cli worker -h
```
