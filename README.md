# immudb Client for Java

Official immudb client for Java 1.8 and above.

Client-Server with [grpc] is hidden and a transport agnostic API is provided to the final application.

[grpc]: https://grpc.io/


### Caveat: a very early start of the implementation!


### Running immudb

Client assumes an already running immudb server. It can be as simple as downloading official binaries or running immudb docker container.

https://immudb.io/docs/quickstart.html

### Using the SDK

The following code snippet shows how to create a synchronous client and run some basic operations

```java
    String TEST_HOSTNAME = "localhost";
    int TEST_PORT = 3322;

    ImmuClient immuClient = ImmuClient.ImmuClientBuilder.newBuilder(TEST_HOSTNAME, TEST_PORT).build();

    immuClient.login("immudb", "");

    byte[] v0 = new byte[] {0, 1, 2, 3};
    byte[] v1 = new byte[] {3, 2, 1, 0};

    immuClient.set("k0", v0);
    immuClient.set("k1", v1);

    byte[] rv0 = immuClient.get("k0");
    byte[] rv1 = immuClient.get("k1");

    Assert.assertEquals(v0, rv0);
    Assert.assertEquals(v1, rv1);

    byte[] sv0 = immuClient.safeGet("k0");
    byte[] sv1 = immuClient.safeGet("k1");

    Assert.assertEquals(sv0, v0);
    Assert.assertEquals(sv1, v1);

    byte[] v2 = new byte[] {0, 1, 2, 3};

    immuClient.safeSet("k2", v2);
    byte[] sv2 = immuClient.safeGet("k2");
    Assert.assertEquals(v2, sv2);

    immuClient.logout();
```