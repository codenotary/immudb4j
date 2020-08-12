# immudb Client for Java

Official immudb client for Java 1.8 and above.

Client-Server with [grpc] is hidden and a transport agnostic API is provided to the final application.

[grpc]: https://grpc.io/


## Using the SDK

### Running immudb

Client assumes an already running immudb server. It can be as simple as downloading official binaries or running immudb docker container.

https://immudb.io/docs/quickstart.html


### Including dependency

Include immudb4j as a dependency in your project:

```xml
    <dependency>
        <groupId>io.codenotary</groupId>
        <artifactId>immudb4j</artifactId>
        <version>0.1.3</version>
    </dependency> 
```

### Code snippet

The following code snippet shows how to create a client and run some basic operations

```java
    ImmuClient immuClient = ImmuClient.newBuilder().build();

    immuClient.login("immudb", "");

    byte[] v0 = new byte[] {0, 1, 2, 3};
    immuClient.set("k0", v0);

    byte[] rv0 = immuClient.get("k0");
    Assert.assertEquals(v0, rv0);

    byte[] sv0 = immuClient.safeGet("k0");
    Assert.assertEquals(sv0, v0);

    immuClient.logout();
```

### Hello Immutable World!

A simple java application using the sdk can be found at https://github.com/codenotary/immudb-client-examples/tree/master/java