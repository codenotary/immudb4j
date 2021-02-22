#!/bin/sh

## NOTE: You need to pass the Coveralls.io's repo token to this script
## (gathered in this case from https://coveralls.io/github/dxps/immudb4j)
## provided as env var in this script like this:
## COVERALLS_REPO_TOKEN=9lss...B0u4 ./gradlew clean test jacocoTestReport coveralls

./gradlew clean test jacocoTestReport coveralls
