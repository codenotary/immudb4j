#!/bin/sh

echo
echo " Running the Unit Tests sequentially, one by one."
echo

# -----------------------------------------------------------------------------

## Unit Tests

TESTS="${TESTS} BasicImmuClientTest"
TESTS="${TESTS} BasicsTest"
TESTS="${TESTS} CryptoUtilsTest"
TESTS="${TESTS} ExceptionsTest"
TESTS="${TESTS} FileImmuStateHolderTest"
TESTS="${TESTS} HealthCheckAndIndexCompactionTest"
TESTS="${TESTS} HistoryTest"
TESTS="${TESTS} HTreeTest"
TESTS="${TESTS} ListDatabasesTest"
TESTS="${TESTS} ListUsersTest"
TESTS="${TESTS} MultidatabaseTest"
TESTS="${TESTS} MultithreadTest"
TESTS="${TESTS} ReferenceTest"
TESTS="${TESTS} ScanTest"
TESTS="${TESTS} SetAllAndGetAllTest"
TESTS="${TESTS} SetAndGetTest"
TESTS="${TESTS} StreamSetAndGetTest"
TESTS="${TESTS} StreamSetAllTest"
TESTS="${TESTS} ShutdownTest"
TESTS="${TESTS} StateTest"
TESTS="${TESTS} TxTest"
TESTS="${TESTS} UserMgmtTest"
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
