# immudb4j [![License](https://img.shields.io/github/license/codenotary/immudb4j)](LICENSE)

[![Slack](https://img.shields.io/badge/join%20slack-%23immutability-brightgreen.svg)](https://slack.vchain.us/)
[![Discuss at immudb@googlegroups.com](https://img.shields.io/badge/discuss-immudb%40googlegroups.com-blue.svg)](https://groups.google.com/group/immudb)
[![Coverage](https://coveralls.io/repos/github/codenotary/immudb4j/badge.svg?branch=master)](https://coveralls.io/github/codenotary/immudb4j?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/io.codenotary/immudb4j.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22io.codenotary%22%20AND%20a:%22immudb4j%22)

### The Official [immudb] Client for Java 1.8 and above.

[immudb]: https://immudb.io/


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
    * [Standard Read and Write](#standard-read-and-write)
    * [Verified or Safe read and write](#verified-or-safe-read-and-write)
    * [Multi-key read and write](#multi-key-read-and-write)
    * [Closing the client](#creating-a-database)
- [Contributing](#contributing)

## Introduction

immudb4j implements a [gRPC] immudb client, based on [immudb's official protobuf definition].<br/>
It exposes a minimal and simple to use API for applications, while the cryptographic verifications and state update protocol implementation 
are fully implemented internally by this client.

The latest validated immudb state may be kept in the local file system using default `FileImmuStateHolder`.<br/>
Please read [immudb Research Paper] for details of how immutability is ensured by [immudb].

[gRPC]: https://grpc.io/
[immudb Research Paper]: https://immudb.io/
[immudb]: https://immudb.io/
[immudb's official protobuf definition](https://github.com/codenotary/immudb/blob/master/pkg/api/schema/schema.proto)

## Prerequisites

immudb4j assumes you have access to a running immudb server.<br/>
Running `immudb` on your system is very simple, please refer to this [immudb QuickStart](https://docs.immudb.io/master/quickstart.html) page.

## Installation

Just include immudb4j as a dependency in your project:
- if using Maven:
  ```xml
  <dependency>
      <groupId>io.codenotary</groupId>
      <artifactId>immudb4j</artifactId>
      <version>1.0.1</version>
  </dependency> 
  ```
- if using Gradle:
  ```groovy
  compile 'io.codenotary:immudb4j:1.0.1'
  ```

`immudb4j` is currently hosted on both [Maven Central].

[Maven Central]: https://search.maven.org/artifact/io.codenotary/immudb4j

## Supported Versions

immudb4j supports the [latest immudb server] release.

[latest immudb server]: https://github.com/codenotary/immudb/releases/tag/v1.5.0

## Quickstart

[Hello Immutable World!] example can be found in `immudb-client-examples` repo.

[Hello Immutable World!]: https://github.com/codenotary/immudb-client-examples/tree/master/java

Follow its [README](https://github.com/codenotary/immudb-client-examples/blob/master/java/README.md) to build and run it.

## Step-by-step Guide

### Creating a Client

The following code snippets show how to create a client.

Using default configuration:
```java
    ImmuClient immuClient = ImmuClient.newBuilder().build();
```

Setting `immudb` url and port:
```java
    ImmuClient immuClient = ImmuClient.newBuilder()
                                .withServerUrl("localhost")
                                .withServerPort(3322)
                                .build();
```

Customizing the `State Holder`:
```java
    FileImmuStateHolder stateHolder = FileImmuStateHolder.newBuilder()
                                        .withStatesFolder("./my_immuapp_states")
                                        .build();

    ImmuClient immuClient = ImmuClient.newBuilder()
                                      .withStateHolder(stateHolder)
                                      .build();
```

### User Sessions

Use `openSession` and `closeSession` methods to initiate and terminate user sessions:

```java
    immuClient.openSession("defaultdb", "usr1", "pwd1");

    // Interact with immudb using open session.
    //...

    immuClient.closeSession();
```

### Creating a Database

Creating a new database is quite simple:

```java
    immuClient.createDatabase("db1");
```

### Standard Read and Write

immudb provides standard read and write operations that behave as in a standard
key-value store i.e. no cryptographic verification is involved. Such operations
may be used when validations can be postponed.

```java
    client.set("k123", new byte[]{1, 2, 3});
    
    byte[] v = client.get("k123").getValue();
```

### Verified or Safe Read and Write

immudb provides built-in cryptographic verification for any entry. The client
implements the mathematical validations while the application uses as a standard
read or write operation:

```java
    try {
        client.verifiedSet("k123", new byte[]{1, 2, 3});
    
        byte[] v = client.verifiedGet("k123").getValue();

    } (catch VerificationException e) {

        // Check if it is a data tampering detected case!

    }
```

### Multi-key Read and Write

Transactional multi-key read and write operations are supported by immudb and immudb4j.

Atomic multi-key write (all entries are persisted or none):

```java
        final List<KVPair> kvs = KVListBuilder.newBuilder()
            .add(new KVPair("sga-key1", new byte[] {1, 2}))
            .add(new KVPair("sga-key2", new byte[] {3, 4}))
            .entries();

        try {
            immuClient.setAll(kvs);
        } catch (CorruptedDataException e) {
            // ...
        }
```

Atomic multi-key read (all entries are retrieved or none):

```java
    List<String> keys = Arrays.asList(key1, key2, key3);
    List<Entry> result = immuClient.getAll(keys);

    for (Entry entry : result) {
        byte[] key = entry.getKey();
        byte[] value = entry.getValue();
        // ...
    }
```

### Closing the client

Apart from the `closeSession`, for closing the connection with immudb server use the `shutdown` operation:
 
```java
    immuClient.shutdown();
```

Note: After the shutdown, a new client needs to be created to establish a new connection.

## Contributing

We welcome contributions. Feel free to join the team!

To report bugs or get help, use [GitHub's issues].

[GitHub's issues]: https://github.com/codenotary/immudb4j/issues
