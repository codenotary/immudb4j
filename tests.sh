#!/bin/sh

echo
echo " Running the Unit Tests sequentially, one by one."
echo

# -----------------------------------------------------------------------------

## Unit Tests

TESTS="${TESTS} BasicImmuClientTest"
TESTS="${TESTS} HTreeTest"
TESTS="${TESTS} ListDatabasesTest ListUsersTest"
TESTS="${TESTS} LoginAndHealthCheckTest"
TESTS="${TESTS} MultidatabaseTest MultithreadTest"
TESTS="${TESTS} ReferenceTest"
TESTS="${TESTS} ScanAndHistoryTest"
TESTS="${TESTS} SetAndGetTest SetAllAndGetAllTest"
TESTS="${TESTS} ShutdownTest"
TESTS="${TESTS} StateTest"
TESTS="${TESTS} TxTest"
TESTS="${TESTS} UseDatabaseTest UserMgmtTest"
TESTS="${TESTS} VerifiedSetAndGetTest"
TESTS="${TESTS} ZAddTest"

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
