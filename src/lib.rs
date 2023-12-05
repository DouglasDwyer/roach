#![allow(clippy::type_complexity)]
#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use crate::private::*;
use redb::*;
use serde::*;
use std::any::*;
use std::cell::*;
use std::marker::*;
use std::mem::*;
use thiserror::*;
use wasm_sync::*;

mod bytemuck;

mod rmp_serde;

mod zstd;

pub struct Archive<D> {
    database: Database,
    marker: PhantomData<D>,
}

impl<D> Archive<D> {
    pub fn new(backend: impl StorageBackend) -> Result<Self, ArchiveError> {
        let database = Builder::new()
            .create_with_backend(backend)
            .map_err(ArchiveError::from_io)?;

        Ok(Self {
            database,
            marker: PhantomData,
        })
    }

    pub fn read(&self) -> Result<Transaction<'_, Const, D>, ArchiveError> {
        let txn = self.database.begin_read().map_err(ArchiveError::from_io)?;
        Ok(Transaction::new(txn))
    }

    pub fn write(&self) -> Result<Transaction<'_, Mut, D>, ArchiveError> {
        let txn = self.database.begin_write().map_err(ArchiveError::from_io)?;
        Ok(Transaction::new(txn))
    }
}

pub struct Transaction<'a, M: Mutability, D> {
    tables: UnsafeCell<Option<RawTableNode<M>>>,
    table_lock: RwLock<()>,
    txn: Option<Box<M::TransactionType<'a>>>,
    marker: PhantomData<D>,
}

impl<'a, M: Mutability, D> Transaction<'a, M, D> {
    fn new(txn: M::TransactionType<'a>) -> Self {
        Self {
            tables: UnsafeCell::default(),
            table_lock: RwLock::new(()),
            txn: Some(Box::new(txn)),
            marker: PhantomData,
        }
    }

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

    fn get_table<K: 'static + ?Sized>(&self) -> Result<Option<&M::TableType<'static, 'static, <<<D as ArchiveType<K>>::Key as BinaryConverter<'static>>::ByteOutput as IntoBytes>::RefType, <<D as ArchiveType<K>>::Value as DataLoad<'static>>::RawValueType>>, ArchiveError> where D: ArchiveType<K>{
        unsafe {
            let table_def = TableDefinition::<'static, <<<D as ArchiveType<K>>::Key as BinaryConverter<'static>>::ByteOutput as IntoBytes>::RefType, <<D as ArchiveType<K>>::Value as DataLoad<'static>>::RawValueType>::new(D::type_name());
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
    }

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
    pub fn commit(mut self) -> Result<(), ArchiveError> {
        unsafe {
            self.drop_tables();
            take(&mut self.txn)
                .expect("Could not get transaction.")
                .commit()
                .map_err(ArchiveError::from_io)
        }
    }

    pub fn set<'b, K: 'static + ?Sized, V: 'b + ?Sized>(&'b mut self, key: &K, value: &V) -> Result<(), ArchiveError> where D: ArchiveType<K>, <D as ArchiveType<K>>::Value: BinaryConverter<'b, Target = V>,
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
            table
                .insert(
                    raw_key.as_ref(),
                    transmute::<_, &'static _>(&raw_value.as_ref()),
                )
                .map_err(ArchiveError::from_io)?;
            Ok(())
        }
    }

    pub fn swap<'b, K: 'static + ?Sized, V: 'b + ?Sized>(&'b mut self, key: &K, value: &V) -> Result<Option<<<D as ArchiveType<K>>::Value as DataLoad<'_>>::OutputType>, ArchiveError> where D: ArchiveType<K>, <D as ArchiveType<K>>::Value: BinaryConverter<'b, Target = V>,
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

    fn get_table_mut<K: 'static + ?Sized>(
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
        unsafe {
            let table_def = TableDefinition::<'static, <<<D as ArchiveType<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType, <<D as ArchiveType<K>>::Value as DataLoad<'a>>::RawValueType>::new(D::type_name());
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
}

unsafe impl<'a, M: Mutability, D> Send for Transaction<'a, M, D> { }
unsafe impl<'a, M: Mutability, D> Sync for Transaction<'a, M, D> { }

impl<'a, M: Mutability, D> Drop for Transaction<'a, M, D> {
    fn drop(&mut self) {
        unsafe {
            if self.txn.is_some() {
                self.drop_tables();
            }
        }
    }
}

pub trait DataConverter: for<'a> DataTransform<'a, &'a Self::Target, ToArchive> {
    type Target: 'static + ?Sized;
}

impl DataConverter for [u8] {
    type Target = Self;
}

pub trait DataTransform<'a, I: 'a + ?Sized, D: Direction> {
    type Output;

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

pub trait ArchiveType<K: 'static + ?Sized> {
    type Key: for<'a> BinaryConverter<'a, Target = K> + ?Sized;
    type Value: for<'a> DataLoad<'a> + ?Sized;

    fn type_name() -> &'static str {
        trim_name(type_name::<K>())
    }
}

pub trait ToWriter {
    fn write<W: std::io::Write>(self, writer: W) -> Result<(), ArchiveError>;
}

impl<'a> ToWriter for &'a [u8] {
    fn write<W: std::io::Write>(self, mut writer: W) -> Result<(), ArchiveError> {
        writer.write_all(self).map_err(ArchiveError::from_io)
    }
}

pub struct AccessGuard<'a, V: RedbValue>(redb::AccessGuard<'a, V>);

impl<'a, T: std::fmt::Debug + RedbValue> std::fmt::Debug for AccessGuard<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AccessGuard").field(&self.0.value()).finish()
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

pub struct Const;

pub struct Mut;

pub struct ToArchive;

pub struct FromArchive;

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("Error interacting with database: {0}")]
    Io(Box<dyn Send + Sync + std::error::Error>),
    #[error("Error serializing data: {0}")]
    Serialize(Box<dyn Send + Sync + std::error::Error>),
    #[error("Error deserializing data: {0}")]
    Deserialize(Box<dyn Send + Sync + std::error::Error>),
}

impl ArchiveError {
    pub fn from_io(x: impl 'static + Send + Sync + std::error::Error) -> Self {
        Self::Io(x.into())
    }

    pub fn from_serialize(x: impl 'static + Send + Sync + std::error::Error) -> Self {
        Self::Serialize(x.into())
    }

    pub fn from_deserialize(x: impl 'static + Send + Sync + std::error::Error) -> Self {
        Self::Deserialize(x.into())
    }
}

struct RawTable<M: Mutability> {
    table: M::TableType<'static, 'static, (), ()>,
    dropper: unsafe fn(*mut ()),
}

impl<M: Mutability> RawTable<M> {
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

    pub unsafe fn cast<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
        &self,
    ) -> &M::TableType<'a, 'b, K, V> {
        &*((&self.table as *const _) as *const M::TableType<'a, 'b, K, V>)
    }

    pub unsafe fn cast_mut<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
        &mut self,
    ) -> &mut M::TableType<'a, 'b, K, V> {
        &mut *((&mut self.table as *mut _) as *mut M::TableType<'a, 'b, K, V>)
    }

    pub unsafe fn drop(self) {
        let mut raw_table = MaybeUninit::new(self.table);
        (self.dropper)(&mut raw_table as *mut MaybeUninit<_> as *mut _);
    }
}

struct RawTableNode<M: Mutability> {
    pub id: TypeId,
    pub table: RawTable<M>,
    pub next: UnsafeCell<Option<Box<RawTableNode<M>>>>,
}

fn trim_name(name: &'static str) -> &'static str {
    if let Some(last) = name.rfind(':') {
        &name[last..]
    } else {
        name
    }
}

mod private {
    use super::*;

    pub trait Direction {}

    impl Direction for FromArchive {}
    impl Direction for ToArchive {}

    pub trait Mutability {
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>: ReadableTable<
            K,
            V,
        >;
        type TransactionType<'a>;

        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(
            x: &'a Self::TransactionType<'b>,
            definition: TableDefinition<'_, K, V>,
        ) -> Result<Option<Self::TableType<'a, 'b, K, V>>, ArchiveError>;
    }

    impl Mutability for Const {
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

    impl Mutability for Mut {
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

    pub trait IntoBytes {
        type ByteType: AsByteRef<Self::RefType>;
        type RefType: 'static + RedbKey;

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

    pub trait AsByteRef<T: RedbKey> {
        fn as_ref(&self) -> T::SelfType<'_>;
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

    pub trait BinaryConverter<'a>: DataConverter {
        type ByteOutput: IntoBytes;
        type ByteConverter: DataTransform<'a, &'a Self::Target, ToArchive, Output = Self::ByteOutput>
            + ?Sized;
        type ValueOutput;
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

    pub trait DataLoad<'a> {
        type OutputType;
        type OutputConverter: DataTransform<
                'a,
                AccessGuard<'a, Self::RawValueType>,
                FromArchive,
                Output = Self::OutputType,
            > + ?Sized;
        type RawValueType: 'static + RedbKey;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MyDb;

    impl ArchiveType<[u8; 3]> for MyDb {
        type Key = [u8; 3];
        type Value = zstd::Zstd<[u8]>;
    }

    impl ArchiveType<u32> for MyDb {
        type Key = bytemuck::Pod<u32>;
        type Value = zstd::Zstd<rmp_serde::Rmp<String>>;
    }

    impl ArchiveType<i64> for MyDb {
        type Key = bytemuck::Pod<i64>;
        type Value = bytemuck::Pod<u16>;
    }

    #[test]
    fn test() {
        let x = Archive::<MyDb>::new(backends::InMemoryBackend::new()).unwrap();
        let mut txn = x.write().unwrap();
        txn.set(&25u32, &"henlo".to_string()).unwrap();
        txn.set(&28u32, &"fren".to_string()).unwrap();
        txn.set(&40i64, &26).unwrap();
        println!("Swapped {:?}", txn.swap(&25u32, &"goodby".to_string()));
        txn.set(&[2u8, 4, 0], &[31, 24, 7][..]).unwrap();
        txn.commit().unwrap();
        let txn = x.read().unwrap();
        for value in txn.iter::<[u8; 3]>() {
            println!("VALUE {value:?}");
        }
    }
}
