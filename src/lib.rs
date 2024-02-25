#![allow(clippy::type_complexity)]
#![allow(incomplete_features)]
#![feature(ascii_char)]
#![feature(const_heap)]
#![feature(const_mut_refs)]
#![feature(const_option)]
#![feature(const_type_name)]
#![feature(const_intrinsic_copy)]
#![feature(core_intrinsics)]
#![feature(generic_const_exprs)]

#![allow(internal_features)]
#![deny(warnings)]
#![warn(missing_docs)]
#![warn(clippy::missing_docs_in_private_items)]

//! #### Rust object archive: strongly-typed persistent key value storage
//! 
//! `roach` is a thin wrapper over [`redb`](https://crates.io/crates/redb) which allows loading and storing strongly-typed data in a customizable fashion. It is ideal for applications which need to persistently store program state or other data in a variety of ways (such as in a serialized or compressed format). The following is a simple example of how to use `roach`:
//! 
//! ```rust
//! use roach::*;
//! 
//! // Define an archive type.
//! struct MyArchive;
//! 
//! // Define a key-value pair which can be stored in the archive.
//! impl ArchiveType<str> for MyArchive {
//!     // The keys will be of type string, and will be stored simply by copying the bytes.
//!     type Key = str;
//!     // The values will be of type [u8], and will be compressed using zstd before storage.
//!     type Value = Zstd<[u8]>;
//! }
//! 
//! // Define another key type.
//! impl ArchiveType<u8> for MyArchive {
//!     type Key = Pod<u8>;
//!     type Value = [u8; 5];
//! }
//! 
//! // Create a new archive.
//! let archive = Archive::<MyArchive>::new(backend::InMemoryBackend::new()).unwrap();
//! // Begin a new atomic transaction.
//! let mut write_txn = archive.write().unwrap();
//! // Store something to the str table.
//! write_txn.set("henlo", &[5, 10, 15]).unwrap();
//! // Commit the transaction.
//! write_txn.commit().unwrap();
//! 
//! // Read the data back to verify that it was saved.
//! let read_txn = archive.read().unwrap();
//! assert_eq!(&read_txn.get("henlo").unwrap().unwrap(), &[5, 10, 15]);
//! ```
//! 
//! ### Optional features
//! 
//! **bytemuck** - Enables the `Pod` data transform, which reads the bytes of a type directly to store them.
//! 
//! **rmp_serde** - Enables the `Rmp` data transform, which serializes Rust types to bytes and back.
//! 
//! **zstd** - Enables the `Zstd` data transform, which compresses data before it is stored.

use crate::private::*;
pub use mutability_marker::{Const, Mut};
use redb::*;
/// Provides backends to store archive data in various ways.
pub use redb::backends as backend;
pub use redb::StorageBackend;
use semver::*;
use std::any::*;
use std::cell::*;
use std::marker::*;
use std::mem::*;
use std::ops::*;
use std::slice::*;
use thiserror::*;
use wasm_sync::*;

#[cfg(feature = "bytemuck")]
/// Implements byte-wise conversion.
mod bytemuck;
#[cfg(feature = "bytemuck")]
pub use bytemuck::*;

#[cfg(feature = "rmp-serde")]
/// Implements MessagePack-based conversation.
mod rmp_serde;
#[cfg(feature = "rmp-serde")]
pub use rmp_serde::*;


#[cfg(feature = "zstd")]
/// Implements compression-based conversion.
mod zstd;
#[cfg(feature = "zstd")]
pub use zstd::*;

/// An archive of strongly-typed data.
pub struct Archive<D> {
    /// The underlying database.
    database: Database,
    /// Marker data for the unused type.
    marker: PhantomData<D>,
}

impl<D> Archive<D> {
    /// The definition of the table used to store the archive version.
    const VERSION_TABLE: TableDefinition<'static, (), &'static str> = TableDefinition::new("__VERSION__");

    /// Opens an archive with the specified storage backend.
    pub fn new(backend: impl StorageBackend) -> Result<Self, ArchiveError> {
        let database = Builder::new()
            .create_with_backend(backend)
            .map_err(ArchiveError::from_io)?;

        Ok(Self {
            database,
            marker: PhantomData,
        })
    }

    /// Gets the application-defined archive version.
    pub fn version(&self) -> Result<Version, ArchiveError> {
        let txn = self.database.begin_read().map_err(ArchiveError::from_io)?;
        let table = txn.open_table(Self::VERSION_TABLE).map_err(ArchiveError::from_io)?;
        let result = table.get(()).map_err(ArchiveError::from_io)?.map(|x| Version::parse(x.value()).map_err(ArchiveError::from_serialize)).unwrap_or(Ok(Version::new(0, 0, 0)));
        result
    }

    /// Sets the application-defined archive version.
    pub fn set_version(&self, version: &Version) -> Result<(), ArchiveError> {
        let txn = self.database.begin_write().map_err(ArchiveError::from_io)?;
        let mut table = txn.open_table(Self::VERSION_TABLE).map_err(ArchiveError::from_io)?;
        table.insert((), version.to_string().as_str()).map_err(ArchiveError::from_io)?;
        Ok(())
    }

    /// Initiates a read transaction against the archive.
    pub fn read(&self) -> Result<Transaction<'_, Const, D>, ArchiveError> {
        let txn = self.database.begin_read().map_err(ArchiveError::from_io)?;
        Ok(Transaction::new(txn))
    }

    /// Initiates a write transaction against the archive.
    pub fn write(&self) -> Result<Transaction<'_, Mut, D>, ArchiveError> {
        let txn = self.database.begin_write().map_err(ArchiveError::from_io)?;
        Ok(Transaction::new(txn))
    }
}

impl<D> std::fmt::Debug for Archive<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Archive").finish()
    }
}

/// Facilitates an atomic interaction with an `Archive`.
pub struct Transaction<'a, M: TransactionMutability, D> {
    /// A linked-list of open tables, which all have pointers to the transaction.
    tables: UnsafeCell<Option<RawTableNode<M>>>,
    /// A lock used when adding nodes to the table list.
    table_lock: RwLock<()>,
    /// The underlying database transaction.
    txn: Option<Box<M::TransactionType<'a>>>,
    /// Marker data for the unused type.
    marker: PhantomData<D>,
}

impl<'a, M: TransactionMutability, D> Transaction<'a, M, D> {
    /// Creates a new transaction with no open tables.
    fn new(txn: M::TransactionType<'a>) -> Self {
        Self {
            tables: UnsafeCell::default(),
            table_lock: RwLock::new(()),
            txn: Some(Box::new(txn)),
            marker: PhantomData,
        }
    }

    /// Gets the value with the specified key from the archive, if any.
    pub fn get<K: 'static + ?Sized>(
        &self,
        key: &K,
    ) -> Result<Option<<<D as ArchiveType<K>>::Value as DataLoad<'_>>::OutputType>, ArchiveError>
    where
        D: ArchiveType<K>,
    {
        unsafe {
            if let Some(table) = self.get_table::<K>()? {
                let raw_value = table.get(<<<D as ArchiveType<K>>::Key as BinaryConverter<'static>>::ByteConverter>::apply(transmute(key))?.into_db_value()?.as_ref()).map_err(ArchiveError::from_io)?;
                if let Some(value) = raw_value {
                    Ok(Some(transmute(
                        <<<D as ArchiveType<K>>::Value as DataLoad<'a>>::OutputConverter>::apply(
                            transmute(value),
                        )?,
                    )))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
    }

    /// Obtains an iterator over all keys of the given type in the archive.
    pub fn keys<K: 'static + ?Sized>(
        &self,
    ) -> impl '_
           + Iterator<
        Item = Result<
            <<D as ArchiveType<K>>::Key as BinaryConverter<'_>>::ValueOutput,
            ArchiveError,
        >,
    >
    where
        D: ArchiveType<K>,
    {
        unsafe {
            let iter_table = self.get_table::<K>().and_then(|table| {
                if let Some(tbl) = table {
                    Ok(Some(tbl.iter().map_err(ArchiveError::from_io)?))
                } else {
                    Ok(None)
                }
            });
            let (ok, err) = match iter_table {
                Ok(val) => (Some(val), None),
                Err(val) => (None, Some(val)),
            };

            err.map(Err).into_iter().chain(
                ok.and_then(std::convert::identity)
                    .into_iter()
                    .flatten()
                    .map(|x| {
                        x.map_err(ArchiveError::from_io).and_then(|(k, _)| {
                            Ok(
                                transmute(<<<D as ArchiveType<K>>::Key as BinaryConverter<
                                    'static,
                                >>::ValueConverter>::apply(
                                    transmute(k)
                                )?),
                            )
                        })
                    }),
            )
        }
    }

    /// Obtains an iterator over all values for the provided key type in the archive.
    pub fn values<K: 'static + ?Sized>(
        &self,
    ) -> impl '_
           + Iterator<
        Item = Result<<<D as ArchiveType<K>>::Value as DataLoad<'_>>::OutputType, ArchiveError>,
    >
    where
        D: ArchiveType<K>,
    {
        unsafe {
            let iter_table = self.get_table::<K>().and_then(|table| {
                if let Some(tbl) = table {
                    Ok(Some(tbl.iter().map_err(ArchiveError::from_io)?))
                } else {
                    Ok(None)
                }
            });
            let (ok, err) = match iter_table {
                Ok(val) => (Some(val), None),
                Err(val) => (None, Some(val)),
            };

            err.map(Err).into_iter().chain(
                ok.and_then(std::convert::identity)
                    .into_iter()
                    .flatten()
                    .map(|x| {
                        x.map_err(ArchiveError::from_io).and_then(|(_, v)| {
                            Ok(transmute(<<<D as ArchiveType<K>>::Value as DataLoad<
                                'static,
                            >>::OutputConverter>::apply(
                                transmute(v)
                            )?))
                        })
                    }),
            )
        }
    }

    /// Obtains an iterator over all keys and values of the given type in the archive.
    pub fn iter<K: 'static + ?Sized>(
        &self,
    ) -> impl '_
           + Iterator<
        Item = Result<
            (
                <<D as ArchiveType<K>>::Key as BinaryConverter<'_>>::ValueOutput,
                <<D as ArchiveType<K>>::Value as DataLoad<'_>>::OutputType,
            ),
            ArchiveError,
        >,
    >
    where
        D: ArchiveType<K>,
    {
        unsafe {
            let iter_table = self.get_table::<K>().and_then(|table| {
                if let Some(tbl) = table {
                    Ok(Some(tbl.iter().map_err(ArchiveError::from_io)?))
                } else {
                    Ok(None)
                }
            });
            let (ok, err) = match iter_table {
                Ok(val) => (Some(val), None),
                Err(val) => (None, Some(val)),
            };

            err.map(Err).into_iter().chain(
                ok.and_then(std::convert::identity)
                    .into_iter()
                    .flatten()
                    .map(|x| {
                        x.map_err(ArchiveError::from_io).and_then(|(k, v)| {
                            Ok(
                                (
                                    transmute(<<<D as ArchiveType<K>>::Key as BinaryConverter<
                                        'static,
                                    >>::ValueConverter>::apply(
                                        transmute(k)
                                    )?),
                                    transmute(<<<D as ArchiveType<K>>::Value as DataLoad<
                                        'static,
                                    >>::OutputConverter>::apply(
                                        transmute(v)
                                    )?),
                                ),
                            )
                        })
                    }),
            )
        }
    }

    /// Gets an immutable reference to a table, opening the table if it was not already opened.
    /// 
    /// # Safety
    /// 
    /// This function may not be called after the transaction has been dropped, or its behavior is undefined.
    unsafe fn get_table<K: 'static + ?Sized>(&self) -> Result<Option<&M::TableType<'static, 'static, <<<D as ArchiveType<K>>::Key as BinaryConverter<'static>>::ByteOutput as IntoBytes>::RefType, <<D as ArchiveType<K>>::Value as DataLoad<'static>>::RawValueType>>, ArchiveError> where D: ArchiveType<K>{
        let table_def = TableDefinition::<'static, <<<D as ArchiveType<K>>::Key as BinaryConverter<'static>>::ByteOutput as IntoBytes>::RefType, <<D as ArchiveType<K>>::Value as DataLoad<'static>>::RawValueType>::new(D::TYPE_NAME);
        let guard = self
            .table_lock
            .read()
            .expect("Could not acquire table lock.");
        if let Some(table) = &*self.tables.get() {
            if table.id == TypeId::of::<K>() {
                Ok(Some(table.table.cast()))
            } else {
                let mut base_node = &table.next;
                while let Some(table) = &*base_node.get() {
                    if table.id == TypeId::of::<K>() {
                        return Ok(Some(table.table.cast()));
                    } else {
                        base_node = &table.next;
                    }
                }

                drop(guard);
                #[allow(unused)]
                let write_guard = self
                    .table_lock
                    .write()
                    .expect("Could not acquire table lock.");
                Ok(M::open_table(
                    transmute(&**self.txn.as_ref().expect("Could not get transaction.")),
                    table_def,
                )?
                .map(|table| {
                    (*base_node.get())
                        .insert(Box::new(RawTableNode {
                            id: TypeId::of::<K>(),
                            table: RawTable::new(table),
                            next: UnsafeCell::default(),
                        }))
                        .table
                        .cast()
                }))
            }
        } else {
            drop(guard);
            #[allow(unused)]
            let write_guard = self
                .table_lock
                .write()
                .expect("Could not acquire table lock.");
            Ok(M::open_table(
                transmute(&**self.txn.as_ref().expect("Could not get transaction.")),
                table_def,
            )?
            .map(|table| {
                (*self.tables.get())
                    .insert(RawTableNode {
                        id: TypeId::of::<K>(),
                        table: RawTable::new(table),
                        next: UnsafeCell::default(),
                    })
                    .table
                    .cast()
            }))
        }
    }

    /// Drops all tables in the table list.
    /// 
    /// # Safety
    /// 
    /// This function may not be called more than once, and must be called before the transaction is dropped.
    unsafe fn drop_tables(&mut self) {
        if let Some(table) = take(&mut *self.tables.get_mut()) {
            table.table.drop();
            let mut next_table = table.next.into_inner();
            while let Some(table) = next_table {
                table.table.drop();
                next_table = table.next.into_inner();
            }
        }
    }
}

impl<'a, D> Transaction<'a, Mut, D> {
    /// Commits this transaction to the archive. Any modifications will become visible for subsequent reads and writes.
    pub fn commit(mut self) -> Result<(), ArchiveError> {
        unsafe {
            self.drop_tables();
            take(&mut self.txn)
                .expect("Could not get transaction.")
                .commit()
                .map_err(ArchiveError::from_io)
        }
    }

    /// Sets the provided key to the given value in the archive.
    pub fn set<'b, K: 'static + ?Sized, V: 'b + ?Sized>(&'b mut self, key: &K, value: &V) -> Result<(), ArchiveError> where D: ArchiveType<K>, <D as ArchiveType<K>>::Value: BinaryConverter<'b, Target = V>,
    <<<<D as ArchiveType<K>>::Value as BinaryConverter<'b>>::ByteOutput as private::IntoBytes>::RefType as redb::RedbValue>::SelfType<'b>: std::borrow::Borrow<<<<D as ArchiveType<K>>::Value as DataLoad<'b>>::RawValueType as redb::RedbValue>::SelfType<'b>> {
        unsafe {
            let table = self.get_table_mut::<K>()?;
            let raw_key =
                <<<D as ArchiveType<K>>::Key as BinaryConverter<'a>>::ByteConverter>::apply(
                    transmute(key),
                )?
                .into_db_value()?;
            let raw_value =
                <<<D as ArchiveType<K>>::Value as BinaryConverter<'b>>::ByteConverter>::apply(
                    transmute(value),
                )?
                .into_db_value()?;
            table
                .insert(
                    raw_key.as_ref(),
                    transmute::<_, &'static _>(&raw_value.as_ref()),
                )
                .map_err(ArchiveError::from_io)?;
            Ok(())
        }
    }

    /// Replaces the value of the provided key, returning the old value (if any).
    pub fn replace<'b, K: 'static + ?Sized, V: 'b + ?Sized>(&'b mut self, key: &K, value: &V) -> Result<Option<<<D as ArchiveType<K>>::Value as DataLoad<'_>>::OutputType>, ArchiveError> where D: ArchiveType<K>, <D as ArchiveType<K>>::Value: BinaryConverter<'b, Target = V>,
    <<<<D as ArchiveType<K>>::Value as BinaryConverter<'b>>::ByteOutput as private::IntoBytes>::RefType as redb::RedbValue>::SelfType<'b>: std::borrow::Borrow<<<<D as ArchiveType<K>>::Value as DataLoad<'b>>::RawValueType as redb::RedbValue>::SelfType<'b>>{
        unsafe {
            let table = self.get_table_mut::<K>()?;
            let raw_key =
                <<<D as ArchiveType<K>>::Key as BinaryConverter<'a>>::ByteConverter>::apply(
                    transmute(key),
                )?
                .into_db_value()?;
            let raw_value =
                <<<D as ArchiveType<K>>::Value as BinaryConverter<'b>>::ByteConverter>::apply(
                    transmute(value),
                )?
                .into_db_value()?;
            let maybe_old_value = table
                .insert(
                    raw_key.as_ref(),
                    transmute::<_, &'static _>(&raw_value.as_ref()),
                )
                .map_err(ArchiveError::from_io)?;

            if let Some(old_value) = maybe_old_value {
                Ok(Some(transmute(
                    <<<D as ArchiveType<K>>::Value as DataLoad<'a>>::OutputConverter>::apply(
                        transmute(old_value),
                    )?,
                )))
            } else {
                Ok(None)
            }
        }
    }

    /// Removes the value with the specified key from the archive.
    pub fn remove<K: 'static + ?Sized>(
        &mut self,
        key: &K,
    ) -> Result<Option<<<D as ArchiveType<K>>::Value as DataLoad<'_>>::OutputType>, ArchiveError>
    where
        D: ArchiveType<K>,
    {
        unsafe {
            let table = self.get_table_mut::<K>()?;
            let raw_key =
                <<<D as ArchiveType<K>>::Key as BinaryConverter<'a>>::ByteConverter>::apply(
                    transmute(key),
                )?
                .into_db_value()?;
            let maybe_old_value = table
                .remove(raw_key.as_ref())
                .map_err(ArchiveError::from_io)?;

            if let Some(old_value) = maybe_old_value {
                Ok(Some(transmute(
                    <<<D as ArchiveType<K>>::Value as DataLoad<'a>>::OutputConverter>::apply(
                        transmute(old_value),
                    )?,
                )))
            } else {
                Ok(None)
            }
        }
    }


    /// Gets a mutable reference to a table, opening/creating the table if it was not already opened/created.
    /// 
    /// # Safety
    /// 
    /// This function may not be called after the transaction has been dropped, or its behavior is undefined.
    unsafe fn get_table_mut<K: 'static + ?Sized>(
        &mut self,
    ) -> Result<
        &mut Table<
            'static,
            'static,
            <<<D as ArchiveType<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType,
            <<D as ArchiveType<K>>::Value as DataLoad<'a>>::RawValueType,
        >,
        ArchiveError,
    >
    where
        D: ArchiveType<K>,
    {
        let table_def = TableDefinition::<'static, <<<D as ArchiveType<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType, <<D as ArchiveType<K>>::Value as DataLoad<'a>>::RawValueType>::new(D::TYPE_NAME);
            if let Some(table) = &mut *self.tables.get() {
                if table.id == TypeId::of::<K>() {
                    Ok(table.table.cast_mut())
                } else {
                    let mut base_node = &mut table.next;
                    while let Some(table) = &mut *base_node.get() {
                        if table.id == TypeId::of::<K>() {
                            return Ok(table.table.cast_mut());
                        } else {
                            base_node = &mut table.next;
                        }
                    }

                    Ok((*base_node.get())
                        .insert(Box::new(RawTableNode {
                            id: TypeId::of::<K>(),
                            table: RawTable::new(
                                Mut::open_table(
                                    transmute(
                                        &**self.txn.as_ref().expect("Could not get transaction."),
                                    ),
                                    table_def,
                                )?
                                .expect("Could not create table."),
                            ),
                            next: UnsafeCell::default(),
                        }))
                        .table
                        .cast_mut())
                }
            } else {
                Ok((*self.tables.get())
                    .insert(RawTableNode {
                        id: TypeId::of::<K>(),
                        table: RawTable::new(
                            Mut::open_table(
                                transmute(
                                    &**self.txn.as_ref().expect("Could not get transaction."),
                                ),
                                table_def,
                            )?
                            .expect("Could not create table."),
                        ),
                        next: UnsafeCell::default(),
                    })
                    .table
                    .cast_mut())
            }
    }
}

unsafe impl<'a, M: TransactionMutability, D> Send for Transaction<'a, M, D> {}
unsafe impl<'a, M: TransactionMutability, D> Sync for Transaction<'a, M, D> {}

impl<'a, M: TransactionMutability, D> std::fmt::Debug for Transaction<'a, M, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Transaction").finish()
    }
}

impl<'a, M: TransactionMutability, D> Drop for Transaction<'a, M, D> {
    fn drop(&mut self) {
        unsafe {
            if self.txn.is_some() {
                self.drop_tables();
            }
        }
    }
}

/// Implements the ability to convert from a canonical target type to an archived representation.
pub trait DataConverter: for<'a> DataTransform<'a, &'a Self::Target, ToArchive> {
    /// The key or value type which may be converted.
    type Target: 'static + ?Sized;
}

/// Implements the ability to convert archived data to in-memory data, or vice-versa.
pub trait DataTransform<'a, I: 'a + ?Sized, D: Direction> {
    /// The output type of this transformation.
    type Output;

    /// Attempts to convert the given input type to the output.
    fn apply(input: I) -> Result<Self::Output, ArchiveError>;
}

impl<const N: usize> DataConverter for [u8; N] {
    type Target = Self;
}

impl<'a, const N: usize> DataTransform<'a, &'a [u8; N], ToArchive> for [u8; N] {
    type Output = &'a Self;

    fn apply(input: &'a [u8; N]) -> Result<Self::Output, ArchiveError> {
        Ok(input)
    }
}

impl<'a, const N: usize> DataTransform<'a, AccessGuard<'a, &'static [u8; N]>, FromArchive>
    for [u8; N]
{
    type Output = AccessGuard<'a, &'static [u8; N]>;

    fn apply(input: AccessGuard<'a, &'static [u8; N]>) -> Result<Self::Output, ArchiveError> {
        Ok(input)
    }
}

impl DataConverter for [u8] {
    type Target = Self;
}

impl<'a> DataTransform<'a, &'a [u8], ToArchive> for [u8] {
    type Output = &'a Self;

    fn apply(input: &'a [u8]) -> Result<Self::Output, ArchiveError> {
        Ok(input)
    }
}

impl<'a> DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive> for [u8] {
    type Output = AccessGuard<'a, &'static [u8]>;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, ArchiveError> {
        Ok(input)
    }
}

impl DataConverter for str {
    type Target = str;
}

impl<'a> DataTransform<'a, &'a str, ToArchive> for str {
    type Output = &'a [u8];

    fn apply(input: &'a str) -> Result<Self::Output, ArchiveError> {
        Ok(input.as_bytes())
    }
}

impl<'a> DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive> for str {
    type Output = StringGuard<'a>;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, ArchiveError> {
        std::str::from_utf8(input.as_ref()).map_err(ArchiveError::from_serialize)?;
        Ok(StringGuard(input))
    }
}

/// Provides a view of archived string data.
pub struct StringGuard<'a>(AccessGuard<'a, &'static [u8]>);

impl<'a> std::fmt::Debug for StringGuard<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("StringGuard").field(&&**self).finish()
    }
}

impl<'a> Deref for StringGuard<'a> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        unsafe {
            std::str::from_utf8_unchecked(self.0.as_ref())
        }
    }
}

/// Defines a type of key-value pair which may be stored in an archive.
pub trait ArchiveType<K: 'static + ?Sized> {
    /// The converter for the key type.
    type Key: for<'a> BinaryConverter<'a, Target = K> + ?Sized;
    /// The converter for the value type.
    type Value: for<'a> DataLoad<'a> + ?Sized;

    /// The type name which should be used to identify these key-value pairs in the archive.
    const TYPE_NAME: &'static str = trim_name(type_name::<K>());
}

/// Allows for writing contents to a `Write`.
pub trait ToWriter {
    /// Dumps the contents of `self` into the provided writer.
    fn write<W: std::io::Write>(self, writer: W) -> Result<(), ArchiveError>;
}

impl<'a> ToWriter for &'a [u8] {
    fn write<W: std::io::Write>(self, mut writer: W) -> Result<(), ArchiveError> {
        writer.write_all(self).map_err(ArchiveError::from_io)
    }
}

/// Provides a view of raw archived data.
pub struct AccessGuard<'a, V: RedbValue>(redb::AccessGuard<'a, V>);

impl<'a, T: std::fmt::Debug + RedbValue> std::fmt::Debug for AccessGuard<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AccessGuard").field(&self.0.value()).finish()
    }
}

impl<'a, const N: usize> Deref for AccessGuard<'a, &[u8; N]> {
    type Target = [u8; N];

    fn deref(&self) -> &Self::Target {
        self.0.value()
    }
}

impl<'a> Deref for AccessGuard<'a, &[u8]> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.value()
    }
}

impl<'a, const N: usize> AsRef<[u8; N]> for AccessGuard<'a, &[u8; N]> {
    fn as_ref(&self) -> &[u8; N] {
        self.0.value()
    }
}

impl<'a> AsRef<[u8]> for AccessGuard<'a, &[u8]> {
    fn as_ref(&self) -> &[u8] {
        self.0.value()
    }
}

/// Marks a type that converts objects to their archived representation.
pub struct ToArchive;

/// Marks a type that converts objects from their archived representation.
pub struct FromArchive;

/// Describes an error that occurred while interacting with an archive.
#[derive(Debug, Error)]
pub enum ArchiveError {
    /// There was a problem during an interaction with the raw database.
    #[error("Error interacting with database: {0}")]
    Io(Box<dyn Send + Sync + std::error::Error>),
    /// There was a problem while serializing data to its archived format.
    #[error("Error serializing data: {0}")]
    Serialize(Box<dyn Send + Sync + std::error::Error>),
    /// There was a problem while deserializing data to its in-memory format.
    #[error("Error deserializing data: {0}")]
    Deserialize(Box<dyn Send + Sync + std::error::Error>),
}

impl ArchiveError {
    /// Creates a new `Archive::Io` error variant.
    pub fn from_io(x: impl 'static + Send + Sync + std::error::Error) -> Self {
        Self::Io(x.into())
    }

    /// Creates a new `Archive::Serialize` error variant.
    pub fn from_serialize(x: impl 'static + Send + Sync + std::error::Error) -> Self {
        Self::Serialize(x.into())
    }

    /// Creates a new `Archive::Deserialize` error variant.
    pub fn from_deserialize(x: impl 'static + Send + Sync + std::error::Error) -> Self {
        Self::Deserialize(x.into())
    }
}

/// Holds a table over the course of the transaction.
struct RawTable<M: TransactionMutability> {
    /// The type-erased table.
    table: M::TableType<'static, 'static, (), ()>,
    /// The function that should be used to drop the table.
    dropper: unsafe fn(*mut ()),
}

impl<M: TransactionMutability> RawTable<M> {
    /// Creates a new raw table.
    /// 
    /// # Safety
    /// 
    /// Once created, the raw table must be manually dropped before the lifetime
    /// of the associated transaction completes.
    pub unsafe fn new<K: 'static + RedbKey, V: 'static + RedbValue>(
        table: M::TableType<'_, '_, K, V>,
    ) -> Self {
        debug_assert!(
            size_of::<M::TableType<'static, 'static, (), ()>>()
                == size_of::<M::TableType<'_, '_, K, V>>()
        );
        let value = MaybeUninit::new(table);
        Self {
            table: value
                .as_ptr()
                .cast::<M::TableType<'static, 'static, (), ()>>()
                .read(),
            dropper: |x| x.cast::<M::TableType<'_, '_, K, V>>().drop_in_place(),
        }
    }

    /// Casts this raw table to a strong type.
    /// 
    /// # Safety
    /// 
    /// The specified conversion type must match the table's original type, or
    /// undefined behavior will occur.
    pub unsafe fn cast<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
        &self,
    ) -> &M::TableType<'a, 'b, K, V> {
        &*((&self.table as *const _) as *const M::TableType<'a, 'b, K, V>)
    }

    /// Mutably casts this raw table to a strong type.
    /// 
    /// # Safety
    /// 
    /// The specified conversion type must match the table's original type, or
    /// undefined behavior will occur.
    pub unsafe fn cast_mut<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
        &mut self,
    ) -> &mut M::TableType<'a, 'b, K, V> {
        &mut *((&mut self.table as *mut _) as *mut M::TableType<'a, 'b, K, V>)
    }

    /// Drops the table.
    pub unsafe fn drop(self) {
        let mut raw_table = MaybeUninit::new(self.table);
        (self.dropper)(&mut raw_table as *mut MaybeUninit<_> as *mut _);
    }
}

/// A node in the linked-list of tables for a transaction.
struct RawTableNode<M: TransactionMutability> {
    /// The ID of the key type for the table.
    pub id: TypeId,
    /// The table itself.
    pub table: RawTable<M>,
    /// The next table in the linked-list.
    pub next: UnsafeCell<Option<Box<RawTableNode<M>>>>,
}

/// Gets the unscoped name of a type by stripping it of any preceding namespaces.
const fn trim_name(name: &'static str) -> &'static str {
    assert!(name.is_ascii());
    unsafe {
        let as_ascii = name.as_ascii().unwrap();
        if as_ascii[name.len() - 1].to_char() == '>' {
            let mut i = 0;
            while i < name.len() {
                if as_ascii[i].to_char() == '<' {
                    let beginning = trim_namespace(slice_str(name, 0, i));
                    let rest = trim_name(slice_str(name, i + 1, name.len() - 1));

                    let result = std::intrinsics::const_allocate((beginning.len() + rest.len() + 2) * std::mem::size_of::<u8>(), std::mem::align_of::<u8>());
                    std::ptr::copy_nonoverlapping(beginning.as_ptr(), result, beginning.len());
                    *result.add(beginning.len()) = b'<';
                    std::ptr::copy_nonoverlapping(rest.as_ptr(), result.add(beginning.len() + 1), rest.len());
                    *result.add(beginning.len() + rest.len() + 1) = b'>';
                    return std::str::from_utf8_unchecked(from_raw_parts(result, beginning.len() + rest.len() + 2));
                }
                i += 1;
            }

            panic!("Type name was not formatted correctly.")
        }
        else {
            trim_namespace(name)
        }
    }
}

/// Removes the namespace from a type name.
/// 
/// # Safety
/// 
/// The name must be a valid ASCII string.
const unsafe fn trim_namespace(name: &'static str) -> &'static str {
    let mut i = name.len();
    while i > 0 {
        let next = i - 1;
        if name.as_ascii().unwrap()[next].to_char() == ':' {
            return slice_str(name, i, name.len());
        }
        i = next;
    }
    
    name
}

/// Creates a constant slice of a string.
/// 
/// # Safety
/// 
/// The start and end must be within the string's range.
const unsafe fn slice_str(a: &'static str, start: usize, end: usize) -> &'static str {
    let bytes = from_raw_parts(a.as_bytes().as_ptr().add(start), end - start);
    std::str::from_utf8_unchecked(bytes)
}

/// Marks the direction in which an archive operation occurs.
pub trait Direction: Sealed {}

impl Direction for FromArchive {}
impl Direction for ToArchive {}

impl Sealed for FromArchive {}
impl Sealed for ToArchive {}

/// Marks whether a type is mutable or immutable.
pub trait TransactionMutability: TransactionMutabilityInner {}

impl TransactionMutability for Const {}
impl TransactionMutability for Mut {}

/// Hides implementation details.
mod private {
    use super::*;

    /// Ensures that a trait cannot be externally implemented.
    pub trait Sealed {}

    /// Defines types associated with constant or mutable transactions.
    pub trait TransactionMutabilityInner {
        /// The type of a table associated with this transaction.
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>: ReadableTable<
            K,
            V,
        >;
        /// The type of the underlying database transaction.
        type TransactionType<'a>;

        /// Opens a table for the given transaction.
        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
            x: &'a Self::TransactionType<'b>,
            definition: TableDefinition<'_, K, V>,
        ) -> Result<Option<Self::TableType<'a, 'b, K, V>>, ArchiveError>;
    }

    impl TransactionMutabilityInner for Const {
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue> =
            ReadOnlyTable<'a, K, V>;
        type TransactionType<'a> = ReadTransaction<'a>;

        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
            x: &'a Self::TransactionType<'b>,
            definition: TableDefinition<'_, K, V>,
        ) -> Result<Option<Self::TableType<'a, 'b, K, V>>, ArchiveError> {
            match x.open_table(definition) {
                Ok(res) => Ok(Some(res)),
                Err(TableError::TableDoesNotExist(_)) => Ok(None),
                Err(err) => Err(ArchiveError::from_io(err)),
            }
        }
    }

    impl TransactionMutabilityInner for Mut {
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue> =
            Table<'a, 'b, K, V>;
        type TransactionType<'a> = WriteTransaction<'a>;

        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
            x: &'a Self::TransactionType<'b>,
            definition: TableDefinition<'_, K, V>,
        ) -> Result<Option<Self::TableType<'a, 'b, K, V>>, ArchiveError> {
            x.open_table(definition)
                .map_err(ArchiveError::from_io)
                .map(Some)
        }
    }

    /// Marks a type as being convertable into a byte array.
    pub trait IntoBytes {
        /// The byte representation of the type.
        type ByteType: AsByteRef<Self::RefType>;

        /// The raw database key type.
        type RefType: 'static + RedbKey;

        /// Converts this type into bytes.
        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError>;
    }

    impl<'a> IntoBytes for &'a [u8] {
        type ByteType = Self;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError> {
            Ok(self)
        }
    }

    impl<'a, const N: usize> IntoBytes for &'a [u8; N] {
        type ByteType = Self;
        type RefType = &'static [u8; N];

        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError> {
            Ok(self)
        }
    }

    impl<'a> IntoBytes for &'a str {
        type ByteType = Self;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError> {
            Ok(self)
        }
    }

    impl IntoBytes for Vec<u8> {
        type ByteType = Self;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError> {
            Ok(self)
        }
    }

    impl IntoBytes for String {
        type ByteType = Self;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError> {
            Ok(self)
        }
    }

    /// Marks a type which may be referenced as a byte array.
    pub trait AsByteRef<T: RedbKey> {
        /// Gets a reference to the raw database type associated with this byte array.
        fn as_ref(&self) -> T::SelfType<'_>;
    }

    impl<'a> AsByteRef<&'static [u8]> for &'a str {
        fn as_ref(&self) -> &[u8] {
            self.as_bytes()
        }
    }

    impl<'a> AsByteRef<&'static [u8]> for &'a [u8] {
        fn as_ref(&self) -> &[u8] {
            self
        }
    }

    impl<'a, const N: usize> AsByteRef<&'static [u8; N]> for &'a [u8; N] {
        fn as_ref(&self) -> &[u8; N] {
            self
        }
    }

    impl AsByteRef<&'static [u8]> for Vec<u8> {
        fn as_ref(&self) -> &[u8] {
            &self[..]
        }
    }

    impl AsByteRef<&'static [u8]> for String {
        fn as_ref(&self) -> &[u8] {
            self.as_bytes()
        }
    }

    /// Marks a type that can convert data into binary.
    pub trait BinaryConverter<'a>: DataConverter {
        /// The output of this converter in the forward direction.
        type ByteOutput: IntoBytes;
        /// The converter in the forward direction.
        type ByteConverter: DataTransform<'a, &'a Self::Target, ToArchive, Output = Self::ByteOutput>
            + ?Sized;
        /// The output of this converter in the reverse direction.
        type ValueOutput;
        /// The converter in the reverse direction.
        type ValueConverter: DataTransform<
                'a,
                AccessGuard<'a, <Self::ByteOutput as IntoBytes>::RefType>,
                FromArchive,
                Output = Self::ValueOutput,
            > + ?Sized;
    }

    impl<'a, T: DataConverter + ?Sized> BinaryConverter<'a> for T
    where
        <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output: IntoBytes,
        T: DataTransform<
            'a,
            AccessGuard<
                'a,
                <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType,
            >,
            FromArchive,
        >,
    {
        type ByteOutput = <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output;
        type ByteConverter = Self;
        type ValueOutput = <T as DataTransform<
            'a,
            AccessGuard<
                'a,
                <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType,
            >,
            FromArchive,
        >>::Output;
        type ValueConverter = Self;
    }

    impl<'a, T: BinaryConverter<'a> + ?Sized> DataLoad<'a> for T
    where <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output: IntoBytes,
    T: DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>
    + DataTransform<'a, AccessGuard<'a, <<T as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType>, FromArchive>,
    {
        type OutputType = <T as DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>>::Output;
        type OutputConverter = Self;
        type RawValueType = <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType;
    }

    /// Marks a type which has the ability to load data from an archive with a canonical target type.
    pub trait DataLoad<'a> {
        /// The output type of the loader.
        type OutputType;
        /// The type which converts from the raw type to the output type.
        type OutputConverter: DataTransform<
                'a,
                AccessGuard<'a, Self::RawValueType>,
                FromArchive,
                Output = Self::OutputType,
            > + ?Sized;
        /// The raw byte type used to store the data.
        type RawValueType: 'static + RedbKey;
    }
}