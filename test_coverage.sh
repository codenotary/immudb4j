#!/bin/sh

## INFO: This script runs the tests, does the coverage and publish it to coveralls.io.
## NOTE: You need to pass the coveralls.io's repo token to this script
## provided as env var in this script like this:
## COVERALLS_REPO_TOKEN=9lss...B0u4 ./gradlew clean test jacocoTestReport coveralls

./gradlew clean test jacocoTestReport coveralls

