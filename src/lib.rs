#![allow(incomplete_features)]
#![feature(associated_type_bounds)]
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
    marker: PhantomData<D>
}

impl<D> Archive<D> {
    pub fn new(backend: impl StorageBackend) -> Result<Self, DbError> {
        let database = Builder::new().create_with_backend(backend).map_err(DbError::from_io)?;

        Ok(Self {
            database,
            marker: PhantomData
        })
    }

    pub fn read(&self) -> Result<Transaction<'_, Const, D>, DbError> {
        let txn = self.database.begin_read().map_err(DbError::from_io)?;
        Ok(Transaction::new(txn))
    }

    pub fn write(&self) -> Result<Transaction<'_, Mut, D>, DbError> {
        let txn = self.database.begin_write().map_err(DbError::from_io)?;
        Ok(Transaction::new(txn))
    }
}

pub struct Transaction<'a, M: Mutability, D> {
    tables: UnsafeCell<Option<RawTableNode<M>>>,
    table_lock: RwLock<()>,
    txn: Option<Box<M::TransactionType<'a>>>,
    marker: PhantomData<D>
}

impl<'a, M: Mutability, D> Transaction<'a, M, D> {
    fn new(txn: M::TransactionType<'a>) -> Self {
        Self {
            tables: UnsafeCell::default(),
            table_lock: RwLock::new(()),
            txn: Some(Box::new(txn)),
            marker: PhantomData
        }
    }

    pub fn get<K: 'static>(&self, key: &K) -> Result<Option<<<D as DataTable<K>>::Value as DataLoad<'_>>::OutputType>, DbError> where D: DataTable<K> {
        unsafe {
            if let Some(table) = self.get_table::<K>()? {
                let raw_value = table.get(<<<D as DataTable<K>>::Key as BinaryConverter<'a>>::ByteConverter>::apply(transmute(key))?.into_db_value()?.as_ref()).map_err(DbError::from_io)?;
                if let Some(value) = raw_value {
                    Ok(Some(transmute(<<<D as DataTable<K>>::Value as DataLoad<'a>>::OutputConverter>::apply(transmute(value))?)))
                }
                else {
                    Ok(None)
                }
            }
            else {
                Ok(None)
            }
        }
    }

    fn get_table<K: 'static>(&self) -> Result<Option<&M::TableType<'static, 'static, <<<D as DataTable<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType, <<D as DataTable<K>>::Value as DataLoad<'a>>::RawValueType>>, DbError> where D: DataTable<K> {
        unsafe {
            let table_def = TableDefinition::<'static, <<<D as DataTable<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType, <<D as DataTable<K>>::Value as DataLoad<'a>>::RawValueType>::new(D::table_name());
            let guard = self.table_lock.read().expect("Could not acquire table lock.");
            if let Some(table) = &*self.tables.get() {
                if table.id == TypeId::of::<K>() {
                    Ok(Some(table.table.cast()))
                }
                else {
                    let mut base_node = &table.next;
                    while let Some(table) = &*base_node.get() {
                        if table.id == TypeId::of::<K>() {
                            return Ok(Some(table.table.cast()));
                        }
                        else {
                            base_node = &table.next;
                        }
                    }

                    drop(guard);
                    let write_guard = self.table_lock.write().expect("Could not acquire table lock.");
                    Ok(M::open_table(transmute(&**self.txn.as_ref().expect("Could not get transaction.")), table_def)?.map(|table| (*base_node.get()).insert(Box::new(RawTableNode {
                        id: TypeId::of::<K>(),
                        table: RawTable::new(table),
                        next: UnsafeCell::default()
                    })).table.cast()))
                }
            }
            else {
                drop(guard);
                let write_guard = self.table_lock.write().expect("Could not acquire table lock.");
                Ok(M::open_table(transmute(&**self.txn.as_ref().expect("Could not get transaction.")), table_def)?.map(|table| (*self.tables.get()).insert(RawTableNode {
                    id: TypeId::of::<K>(),
                    table: RawTable::new(table),
                    next: UnsafeCell::default()
                }).table.cast()))
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
    pub fn commit(mut self) -> Result<(), DbError> {
        unsafe {
            self.drop_tables();
            take(&mut self.txn).expect("Could not get transaction.").commit().map_err(DbError::from_io)
        }
    }

    pub fn set<K: 'static, V: 'a>(&mut self, key: &K, value: &V) -> Result<(), DbError> where D: DataTable<K>, <D as DataTable<K>>::Value: BinaryConverter<'a, Target = V>, 
    <<<<D as DataTable<K>>::Value as BinaryConverter<'a>>::ByteOutput as private::IntoBytes>::RefType as redb::RedbValue>::SelfType<'a>: std::borrow::Borrow<<<<D as DataTable<K>>::Value as DataLoad<'a>>::RawValueType as redb::RedbValue>::SelfType<'a>> {
        unsafe {
            let table = self.get_table_mut::<K>()?;
            let raw_key = <<<D as DataTable<K>>::Key as BinaryConverter<'a>>::ByteConverter>::apply(transmute(key))?.into_db_value()?;
            let raw_value = <<<D as DataTable<K>>::Value as BinaryConverter<'a>>::ByteConverter>::apply(transmute(value))?.into_db_value()?;
            table.insert(raw_key.as_ref(), transmute::<_, &'static _>(&raw_value.as_ref())).map_err(DbError::from_io)?;
            Ok(())
        }
    } 
    

    fn get_table_mut<K: 'static>(&mut self) -> Result<&mut Table<'static, 'static, <<<D as DataTable<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType, <<D as DataTable<K>>::Value as DataLoad<'a>>::RawValueType>, DbError> where D: DataTable<K> {
        unsafe {
            let table_def = TableDefinition::<'static, <<<D as DataTable<K>>::Key as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType, <<D as DataTable<K>>::Value as DataLoad<'a>>::RawValueType>::new(D::table_name());
            if let Some(table) = &mut *self.tables.get() {
                if table.id == TypeId::of::<K>() {
                    Ok(table.table.cast_mut())
                }
                else {
                    let mut base_node = &mut table.next;
                    while let Some(table) = &mut *base_node.get() {
                        if table.id == TypeId::of::<K>() {
                            return Ok(table.table.cast_mut());
                        }
                        else {
                            base_node = &mut table.next;
                        }
                    }

                    Ok((*base_node.get()).insert(Box::new(RawTableNode {
                        id: TypeId::of::<K>(),
                        table: RawTable::new(Mut::open_table(transmute(&**self.txn.as_ref().expect("Could not get transaction.")), table_def)?.expect("Could not create table.")),
                        next: UnsafeCell::default()
                    })).table.cast_mut())
                }
            }
            else {
                Ok((*self.tables.get()).insert(RawTableNode {
                    id: TypeId::of::<K>(),
                    table: RawTable::new(Mut::open_table(transmute(&**self.txn.as_ref().expect("Could not get transaction.")), table_def)?.expect("Could not create table.")),
                    next: UnsafeCell::default()
                }).table.cast_mut())
            }
        }
    }
}

impl<'a, M: Mutability, D> Drop for Transaction<'a, M, D> {
    fn drop(&mut self) {
        unsafe {
            if self.txn.is_some() {
                self.drop_tables();
            }
        }
    }
}

pub trait DataTable<K: 'static + ?Sized> {
    type Key: for<'a> BinaryConverter<'a, Target = K>;
    type Value: for<'a> DataLoad<'a>;

    fn table_name() -> &'static str {
        trim_name(type_name::<K>())
    }
}

pub trait DataConverter: for<'a> DataTransform<'a, &'a Self::Target, ToArchive> {
    type Target: 'static;
}

pub trait DataTransform<'a, I: 'a, D: Direction> {
    type Output;

    fn apply(input: I) -> Result<Self::Output, DbError>;
}

pub trait ToWriter {
    fn write<W: std::io::Write>(self, writer: W) -> Result<(), DbError>;
}

pub trait DataLoad<'a>
{
    type OutputType;
    type OutputConverter: DataTransform<'a, AccessGuard<'a, Self::RawValueType>, FromArchive, Output = Self::OutputType>;
    type RawValueType: 'static + RedbKey;

    //fn load<K: 'static + RedbKey>(table: &'a impl ReadableTable<K, Self::RawValueType>, key: K::SelfType<'a>) -> Result<Option<Self::OutputType>, DbError>;
}

/*
pub trait DataStore<'a, I: 'a>: DataLoad<'a> {
    fn remove<K: 'static + RedbKey>(table: &mut Table<'_, '_, K, Self::RawValueType>, key: K::SelfType<'a>) -> Result<<Self as DataLoad<'a>>::OutputType, DbError>;
    fn store<K: 'static + RedbKey>(table: &mut Table<'_, '_, K, Self::RawValueType>, key: K::SelfType<'a>, value: I) -> Result<(), DbError>;
    fn swap<K: 'static + RedbKey>(table: &mut Table<'_, '_, K, Self::RawValueType>, key: K::SelfType<'a>, value: I) -> Result<<Self as DataLoad<'a>>::OutputType, DbError>;
} */

pub trait BinaryConverter<'a>: DataConverter {
    type ByteOutput: IntoBytes;
    type ByteConverter: DataTransform<'a, &'a Self::Target, ToArchive, Output = Self::ByteOutput>;
    type ValueOutput;
}

impl<'a, T: DataConverter> BinaryConverter<'a> for T
where <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output: IntoBytes,
T: DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>
{
    type ByteOutput = <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output;
    type ByteConverter = Self;
    type ValueOutput = <T as DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>>::Output;
}

impl<'a, T: BinaryConverter<'a>> DataLoad<'a> for T
where <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output: IntoBytes,
T: DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>
+ DataTransform<'a, AccessGuard<'a, <<T as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType>, FromArchive>,
{
    type OutputType = <T as DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>>::Output;
    type OutputConverter = Self;//<T as DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>>::Output;
    type RawValueType = <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType;
}

/*
impl<'a, I: 'a, T: BinaryConverter<'a> + DataTransform<'a, I, ToArchive, Output = <Self as BinaryConverter<'a>>::ByteOutput>> DataStore<'a, I> for T
where <T as DataTransform<'a, &'a T::Target, ToArchive>>::Output: IntoBytes,
T: DataTransform<'a, AccessGuard<'a, <<T as DataTransform<'a, &'a T::Target, ToArchive>>::Output as IntoBytes>::RefType>, FromArchive>
+ DataTransform<'a, AccessGuard<'a, <<T as BinaryConverter<'a>>::ByteOutput as IntoBytes>::RefType>, FromArchive>,
{
    fn remove<K: 'static + RedbKey>(table: &mut Table<'_, '_, K, Self::RawValueType>, key: K::SelfType<'a>) -> Result<<Self as DataLoad<'a>>::OutputType, DbError> {
        todo!()
    }

    fn store<K: 'static + RedbKey>(table: &mut Table<'_, '_, K, Self::RawValueType>, key: K::SelfType<'a>, value: I) -> Result<(), DbError> {
        //table.insert(key, Self::apply(value)?.into_db_value()?.as_ref()).map_err(DbError::from_io)?;
        Ok(())
    }

    fn swap<K: 'static + RedbKey>(table: &mut Table<'_, '_, K, Self::RawValueType>, key: K::SelfType<'a>, value: I) -> Result<<Self as DataLoad<'a>>::OutputType, DbError> {
        //let raw_value = table.insert(key, Self::apply(value)?.into_db_value()?.as_ref()).map_err(|x| DbError::from_io)?;
        todo!()
    }
} */

pub struct AccessGuard<'a, V: RedbValue>(redb::AccessGuard<'a, V>);

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
pub enum DbError {
    #[error("Error interacting with database: {0}")]
    Io(Box<dyn Send + Sync + std::error::Error>),
    #[error("Error serializing data: {0}")]
    Serialize(Box<dyn Send + Sync + std::error::Error>),
    #[error("Error deserializing data: {0}")]
    Deserialize(Box<dyn Send + Sync + std::error::Error>)
}

impl DbError {
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
    dropper: unsafe fn(*mut ())
}

impl<M: Mutability> RawTable<M> {
    pub unsafe fn new<K: 'static + RedbKey, V: 'static + RedbValue>(table: M::TableType<'_, '_, K, V>) -> Self {
        debug_assert!(size_of::<M::TableType<'static, 'static, (), ()>>() == size_of::<M::TableType<'_, '_, K, V>>());
        let value = MaybeUninit::new(table);
        Self {
            table: value.as_ptr().cast::<M::TableType<'static, 'static, (), ()>>().read(),
            dropper: |x| x.cast::<M::TableType<'_, '_, K, V>>().drop_in_place()
        }
    }

    pub unsafe fn cast<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(&self) -> &M::TableType<'a, 'b, K, V> {
        &*((&self.table as *const _) as *const M::TableType<'a, 'b, K, V>)
    }

    pub unsafe fn cast_mut<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(&mut self) -> &mut M::TableType<'a, 'b, K, V> {
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
    pub next: UnsafeCell<Option<Box<RawTableNode<M>>>>
}

fn trim_name(name: &'static str) -> &'static str {
    if let Some(last) = name.rfind(":") {
        &name[last..]
    }
    else {
        name
    }
}

mod private {
    use super::*;

    pub trait Direction {}

    impl Direction for FromArchive {}
    impl Direction for ToArchive {}

    pub trait Mutability {
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>: ReadableTable<K, V>;
        type TransactionType<'a>;

        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(x: &'a Self::TransactionType<'b>, definition: TableDefinition<'_, K, V>) -> Result<Option<Self::TableType<'a, 'b, K, V>>, DbError>;
    }

    impl Mutability for Const {
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue> = ReadOnlyTable<'a, K, V>;
        type TransactionType<'a> = ReadTransaction<'a>;

        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(x: &'a Self::TransactionType<'b>, definition: TableDefinition<'_, K, V>) -> Result<Option<Self::TableType<'a, 'b, K, V>>, DbError> {
            match x.open_table(definition) {
                Ok(res) => Ok(Some(res)),
                Err(TableError::TableDoesNotExist(_)) => Ok(None),
                Err(err) => Err(DbError::from_io(err))
            }
        }
    }

    impl Mutability for Mut {
        type TableType<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue> = Table<'a, 'b, K, V>;
        type TransactionType<'a> = WriteTransaction<'a>;

        fn open_table<'a: 'b, 'b, K: 'static + RedbKey, V: 'static + RedbValue>(x: &'a Self::TransactionType<'b>, definition: TableDefinition<'_, K, V>) -> Result<Option<Self::TableType<'a, 'b, K, V>>, DbError> {
            x.open_table(definition).map_err(DbError::from_io).map(Some)
        }
    }

    pub trait IntoBytes {
        type ByteType: AsByteRef<Self::RefType>;
        type RefType: 'static + RedbKey;

        fn into_db_value(self) -> Result<Self::ByteType, DbError>;
    }

    impl<'a> IntoBytes for &'a [u8] {
        type ByteType = Self;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, DbError> {
            Ok(self)
        }
    }

    impl<'a, const N: usize> IntoBytes for &'a [u8; N] {
        type ByteType = Self;
        type RefType = &'static [u8; N];

        fn into_db_value(self) -> Result<Self::ByteType, DbError> {
            Ok(self)
        }
    }

    impl<T: ToWriter> IntoBytes for T {
        type ByteType = Vec<u8>;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, DbError> {
            const DEFAULT_SIZE: usize = usize::BITS as usize;

            let mut result = Vec::with_capacity(DEFAULT_SIZE);
            self.write(&mut result)?;
            Ok(result)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MyDb;
    
    impl DataTable<u32> for MyDb {
        type Key = bytemuck::Pod<u32>;
        type Value = zstd::Zstd<rmp_serde::Rmp<String>>;
    }
    
    impl DataTable<i64> for MyDb {
        type Key = bytemuck::Pod<i64>;
        type Value = bytemuck::Pod<u16>;
    }

    #[test]
    fn test() {
        let x = Archive::<MyDb>::new(backends::InMemoryBackend::new()).unwrap();
        let mut txn = x.write().unwrap();
        txn.set(&25i64, &20u16).unwrap();
        println!("I read {:?}", txn.get(&25i64).unwrap());
        txn.commit().unwrap();
        let txn = x.read().unwrap();
        println!("Get {:?}", txn.get(&25i64).unwrap());
    }
}