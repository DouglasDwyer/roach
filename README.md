# roach

[![Docs.rs](https://img.shields.io/badge/docs-passing-gre)](https://docs.rs/roach)
![Nightly](https://img.shields.io/badge/nightly-required-red)

#### Rust object archive: strongly-typed persistent key value storage

`roach` is a thin wrapper over [`redb`](https://crates.io/crates/redb) which allows loading and storing strongly-typed data in a customizable fashion. It is ideal for applications which need to persistently store program state or other data in a variety of ways (such as in a serialized or compressed format). The following is a simple example of how to use `roach`:

```rust
use roach::*;

// Define an archive type.
struct MyArchive;

// Define a key-value pair which can be stored in the archive.
impl ArchiveType<str> for MyArchive {
    // The keys will be of type string, and will be stored simply by copying the bytes.
    type Key = str;
    // The values will be of type [u8], and will be compressed using zstd before storage.
    type Value = Zstd<[u8]>;
}

// Define another key type.
impl ArchiveType<u8> for MyArchive {
    type Key = Pod<u8>;
    type Value = [u8; 5];
}

// Create a new archive.
let archive = Archive::<MyArchive>::new(backend::InMemoryBackend::new()).unwrap();
// Begin a new atomic transaction.
let mut write_txn = archive.write().unwrap();
// Store something to the str table.
write_txn.set("henlo", &[5, 10, 15]).unwrap();
// Commit the transaction.
write_txn.commit().unwrap();

// Read the data back to verify that it was saved.
let read_txn = archive.read().unwrap();
assert_eq!(&read_txn.get("henlo").unwrap().unwrap(), &[5, 10, 15]);
```

### Optional features

**bytemuck** - Enables the `Pod` data transform, which reads the bytes of a type directly to store them.

**rmp_serde** - Enables the `Rmp` data transform, which serializes Rust types to bytes and back.

**zstd** - Enables the `Zstd` data transform, which compresses data before it is stored.