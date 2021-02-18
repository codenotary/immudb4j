#!/bin/sh

echo
echo " Running the unit tests sequentially, one by one."
echo


for test in BasicImmuClientTest CreateDatabaseTest ListDatabasesTest
do
  echo ""
  echo "======================================="
  echo "  Running ${test}"
  echo "======================================="
  echo ""
  ./gradlew test --info --tests $test
done

