use crate::*;
use crate::rmp_serde::private::*;
use ::rmp_serde::*;
use ::rmp_serde::encode::*;
use serde::de::*;
use std::marker::*;

pub struct Rmp<T: 'static + Serialize + DeserializeOwned>(PhantomData<T>);

impl<T: Serialize + DeserializeOwned> DataConverter for Rmp<T> {
    type Target = T;
}

impl<'a, T: Serialize + DeserializeOwned> DataTransform<'a, &'a T, ToArchive> for Rmp<T> {
    type Output = SerializeWrite<'a, T>;

    fn apply(input: &'a T) -> Result<Self::Output, DbError> {
        Ok(SerializeWrite(input))
    }
}

impl<'a, T: Serialize + DeserializeOwned, I: 'a + std::io::Read> DataTransform<'a, I, FromArchive> for Rmp<T> {
    type Output = T;

    fn apply(input: I) -> Result<Self::Output, DbError> {
        from_read(input).map_err(DbError::from_deserialize)
    }
}

impl<'a, T: Serialize + DeserializeOwned> DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive> for Rmp<T> {
    type Output = T;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, DbError> {
        from_slice(input.as_ref()).map_err(DbError::from_deserialize)
    }
}

mod private {
    use super::*;

    pub struct SerializeWrite<'a, T: Serialize + DeserializeOwned>(pub &'a T);

    impl<'a, T: Serialize + DeserializeOwned> ToWriter for SerializeWrite<'a, T> {
        fn write<W: std::io::Write>(self, mut writer: W) -> Result<(), DbError> {
            write_named(&mut writer, self.0).map_err(DbError::from_serialize)
        }
    }
}