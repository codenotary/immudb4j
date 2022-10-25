#!/bin/sh

echo
echo " Running the Unit Tests sequentially, one by one."
echo

# -----------------------------------------------------------------------------

## Unit Tests

TESTS="${TESTS} BasicImmuClientTest"
TESTS="${TESTS} HistoryTest"
TESTS="${TESTS} HTreeTest"
TESTS="${TESTS} ListDatabasesTest ListUsersTest"
TESTS="${TESTS} LoginAndHealthCheckAndCleanIndexTest"
TESTS="${TESTS} MultidatabaseTest MultithreadTest"
TESTS="${TESTS} ReferenceTest"
TESTS="${TESTS} ScanTest"
TESTS="${TESTS} SetAllAndGetAllTest SetAndGetTest"
TESTS="${TESTS} StateTest"
TESTS="${TESTS} TxTest"
TESTS="${TESTS} UserMgmtTest"
TESTS="${TESTS} VerifiedSetAndGetTest"
TESTS="${TESTS} ZAddTest"
TESTS="${TESTS} ShutdownTest"

# -----------------------------------------------------------------------------

for test in ${TESTS}
do
  echo ""
  echo "======================================="
  echo "  Running ${test}"
  echo "======================================="
  echo ""
  ./gradlew test --info --tests $test
done
