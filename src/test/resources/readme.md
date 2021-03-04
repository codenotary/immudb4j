## Testing with Public and Private Key Files

For testing the server signature checking, a pair of private and public keys needs to be created first. Example:

```shell
# Generate the public and private key pair.
$ openssl ecparam -name prime256v1 -genkey -noout -out test_private_key.pem
$ openssl ec -in test_private_key.pem -pubout -out test_public_key.pem
```

Existing `test_private.key` and `test_public.key` files are the result of such actions,
and are used here for testing purposes (see `CryptoUtilsTest`).

Also, this feature needs to be activated on the server by starting the immudb server with the signing (private) key:

```shell
# Start immudb with signing.
$ ./immudb --signingKey ../src/test/resources/test_private_key.pem
```
