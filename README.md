# immudb [![License](https://img.shields.io/github/license/codenotary/immudb4j)](LICENSE)

[![Slack](https://img.shields.io/badge/join%20slack-%23immutability-brightgreen.svg)](https://slack.vchain.us/)
[![Discuss at immudb@googlegroups.com](https://img.shields.io/badge/discuss-immudb%40googlegroups.com-blue.svg)](https://groups.google.com/group/immudb)
[![Coverage](https://coveralls.io/repos/github/codenotary/immudb4j/badge.svg?branch=master)](https://coveralls.io/github/codenotary/immudb4j?branch=master)

# immudb Client for Java

Official [immudb] client for Java 1.8 and above.

[immudb]: https://grpc.io/


## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Supported Versions](#supported-versions)
- [Quickstart](#quickstart)
- [Client description](#description)
- [Step by step](#step-by-step)
- [Known limitations](#known-limitations)
- [Development](#development)
- [Contributing](#contributing)

## Prerequisites

Client assumes an already running immudb server. It can be as simple as downloading official binaries or running immudb docker container.

https://immudb.io/docs/quickstart.html

## Installation

Client-Server with [grpc] is hidden and a transport agnostic API is provided to the final application.

[grpc]: https://grpc.io/

Include immudb4j as a dependency in your project:

with Maven:
```xml
    <dependency>
        <groupId>io.codenotary</groupId>
        <artifactId>immudb4j</artifactId>
        <version>0.1.3</version>
    </dependency> 
```

with Gradle:
```groovy
    compile 'io.codenotary:immudb4j:0.1.3'
```

immudb4j is currently hosted in [Github Packages].

[Github Packages]: https://docs.github.com/en/packages

immudb4j Github Package repository needs to be included with authentication.
When using maven it means to include immudb4j Github Package in your .m2/settings.xml
file. See "Configuring Apache Maven for use with GitHub Packages" 
and "Configuring Gradle for use with GitHub Packages" at [Github Packages].

## Supported Versions

immudb4j supports the [latest immudb release].

[latest immudb release]: https://github.com/codenotary/immudb/releases/tag/v0.7.0

## Quickstart

[Hello Immutable World!] example can be found in `immudb-client-examples` repo.

[Hello Immutable World!]: https://github.com/codenotary/immudb-client-examples/tree/master/java

Follow its README to build and run it.

## Client description

immudb4j implements a [grpc] immudb client while a simple API is exposed for consumption.
Cryptographic verifications are done as part of any validated (or safe) operation, 
such as `safeGet` or `safeSet`.
Latest validated immudb state may be keep in the local filesystem when using default `FileRootHolder`,
please read [immudb research paper] for details of how immutability is ensured

[grpc]: https://grpc.io/
[immudb research paper]: https://immudb.io/

## Step by step

The following code snippet shows how to create a client and run some basic operations

```java
    ImmuClient immuClient = ImmuClient.newBuilder().build();

    immuClient.login("immudb", "");  // Initiates user session

    byte[] v0 = new byte[] {0, 1, 2, 3};
    immuClient.set("k0", v0);

    byte[] rv0 = immuClient.get("k0");
    Assert.assertEquals(v0, rv0);

    try {
      byte[] v = client.safeGet("k0");
    } catch (VerificationException e) {
      // TODO: tampering detected!
      e.printStackTrace();
    }    

    immuClient.logout();  // Terminate user session

    immuClient.shutdown(); // Closing connection
```

## Known limitations

TODO

## Development

TODO


## Contributing

TODO
