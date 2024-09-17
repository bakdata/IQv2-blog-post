# IQv2 Blog Post

This is the code repository of the blog post [TODO: link and title to blog post].

## Run

First make sure that everything is clean

```bash
./gradlew clean
```

Then, you can run the tests and see the debug logs in the stdout console.
Run tests:

```bash
./gradlew :test
```

## How to navigate and understand the code

First start with the tests and understand the input and expected output of each test case. Afterward, a good starting
point to navigate through the code is the enum
factory [StoreType](https://github.com/bakdata/IQv2-blog-post-private/blob/ec6d3eba818b23dd7734202d39f6b7ed29875ef8/src/main/java/com/bakdata/kafka/example/StoreType.java#L19-L19).
This enum implements and creates each state store's write and read logic.

Most of the state stores use the high-level DSL to materialize the stream into a table. The TimestampedKeyValueStore and
the VersionedStateStore uses the low-level processor API (PAPI) to write the data into the state store.
