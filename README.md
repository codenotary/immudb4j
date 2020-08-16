# immudb [![License](https://img.shields.io/github/license/codenotary/immudb4j)](LICENSE)

[![Slack](https://img.shields.io/badge/join%20slack-%23immutability-brightgreen.svg)](https://slack.vchain.us/)
[![Discuss at immudb@googlegroups.com](https://img.shields.io/badge/discuss-immudb%40googlegroups.com-blue.svg)](https://groups.google.com/group/immudb)
[![Coverage](https://coveralls.io/repos/github/codenotary/immudb4j/badge.svg?branch=master)](https://coveralls.io/github/codenotary/immudb4j?branch=master)

# immudb Client for Java

Official [immudb] client for Java 1.8 and above.

[immudb]: https://grpc.io/


## Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Supported Versions](#supported-versions)
- [Quickstart](#quickstart)
- [Step by step guide](#step-by-step-guide)
    * [Creating a Client](#creating-a-client)
    * [User sessions](#user-sessions)
    * [Creating a database](#creating-a-database)
    * [Setting the active database](#setting-the-active-database)
    * [Traditional read and write](#traditional-read-and-write)
    * [Verified or Safe read and write](#verified-or-safe-read-and-write)
    * [Closing the client](#creating-a-database)
- [Contributing](#contributing)

## Introduction

immudb4j implements a [grpc] immudb client. A minimalist API is exposed for applications while cryptographic
verifications and state update protocol implementation are fully implemented by this client.
Latest validated immudb state may be keep in the local filesystem when using default `FileRootHolder`,
please read [immudb research paper] for details of how immutability is ensured by [immudb].

[grpc]: https://grpc.io/
[immudb research paper]: https://immudb.io/
[immudb]: https://immudb.io/

## Prerequisites

immudb4j assumes an already running immudb server. Running `immudb` is quite simple, please refer to the
following link for downloading and running it: https://immudb.io/docs/quickstart.html

## Installation

Just include immudb4j as a dependency in your project:

if using `Maven`:
```xml
    <dependency>
        <groupId>io.codenotary</groupId>
        <artifactId>immudb4j</artifactId>
        <version>0.1.5</version>
    </dependency> 
```

if using `Gradle`:
```groovy
    compile 'io.codenotary:immudb4j:0.1.5'
```

Note: immudb4j is currently hosted in [Github Packages].

[Github Packages]: https://docs.github.com/en/packages

Thus `immudb4j Github Package repository` needs to be included with authentication.
When using maven it means to include immudb4j Github Package in your `~/.m2/settings.xml`
file. See "Configuring Apache Maven for use with GitHub Packages" 
and "Configuring Gradle for use with GitHub Packages" at [Github Packages].

## Supported Versions

immudb4j supports the [latest immudb release].

[latest immudb release]: https://github.com/codenotary/immudb/releases/tag/v0.7.0

## Quickstart

[Hello Immutable World!] example can be found in `immudb-client-examples` repo.

[Hello Immutable World!]: https://github.com/codenotary/immudb-client-examples/tree/master/java

Follow its `README` to build and run it.

## Step by step guide

### Creating a Client

The following code snippets shows how to create a client.

Using default configuration:
```java
    ImmuClient immuClient = ImmuClient.newBuilder().build();
```

Setting `immudb` url and port:
```java
    ImmuClient immuClient = ImmuClient.newBuilder()
                                .setServerUrl("localhost")
                                .setServerPort(3322)
                                .build();
```

Customizing the `Root Holder`:
```java
    FileRootHolder rootHolder = FileRootHolder.newBuilder()
                                    .setRootsFolder("./my_immuapp_roots")
                                    .build();

    ImmuClient immuClient = ImmuClient.newBuilder()
                                      .withRootHolder(rootHolder)
                                      .build();
```

### User sessions

Use `login` and `logout` methods to initiate and terminate user sessions:

```java
    immuClient.login("usr1", "pwd1");

    // Interact with immudb using logged user

    immuClient.logout();
```

### Creating a database

Creating a new database is quite simple:

```java
    immuClient.createDatabase("db1");
```

### Setting the active database

Specify the active database with:

```java
    immuClient.useDatabase("db1");
```

### Traditional read and write

immudb provides read and write operations that behave as a traditional
key-value store i.e. no cryptographic verification is done. This operations
may be used when validations can be post-poned:

```java
    client.set("k123", new byte[]{1, 2, 3});
    
    byte[] v = client.get("k123");
```

### Verified or Safe read and write

immudb provides built-in cryptographic verification for any entry. The client
implements the mathematical validations while the application uses as a traditional
read or write operation:

```java
    try {
        client.safeSet("k123", new byte[]{1, 2, 3});
    
        byte[] v = client.safeGet("k123");

    } (catch VerificationException e) {

        //TODO: tampering detected!

    }
```

### Closing the client

To programatically close the connection with immudb server use the `shutdown` operation:
 
```java
    immuClient.shutdown();
```

Note: after shutdown, a new client needs to be created to establish a new connection.

## Contributing

We welcome contributions. Feel free to join the team!

To report bugs or get help, use [GitHub's issues].

[GitHub's issues]: https://github.com/codenotary/immudb4j/issues
