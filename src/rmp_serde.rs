use crate::rmp_serde::private::*;
use crate::*;
use ::rmp_serde::encode::*;
use ::rmp_serde::*;
use serde::*;
use serde::de::*;

/// Archives a type by serializing its contents with the MessagePack protocol.
pub struct Rmp<T: 'static + Serialize + DeserializeOwned>(PhantomData<T>);

impl<T: Serialize + DeserializeOwned> DataConverter for Rmp<T> {
    type Target = T;
}

impl<'a, T: Serialize + DeserializeOwned> DataTransform<'a, &'a T, ToArchive> for Rmp<T> {
    type Output = SerializeWrite<'a, T>;

    fn apply(input: &'a T) -> Result<Self::Output, ArchiveError> {
        Ok(SerializeWrite(input))
    }
}

impl<'a, T: Serialize + DeserializeOwned, I: 'a + std::io::Read> DataTransform<'a, I, FromArchive>
    for Rmp<T>
{
    type Output = T;

    fn apply(input: I) -> Result<Self::Output, ArchiveError> {
        from_read(input).map_err(ArchiveError::from_deserialize)
    }
}

impl<'a, T: Serialize + DeserializeOwned>
    DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive> for Rmp<T>
{
    type Output = T;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, ArchiveError> {
        from_slice(input.as_ref()).map_err(ArchiveError::from_deserialize)
    }
}

/// Hides implementation details.
mod private {
    use super::*;

    /// Writes the serialized value of an object to a writer.
    pub struct SerializeWrite<'a, T: Serialize + DeserializeOwned>(pub &'a T);

    impl<'a, T: Serialize + DeserializeOwned> ToWriter for SerializeWrite<'a, T> {
        fn write<W: std::io::Write>(self, mut writer: W) -> Result<(), ArchiveError> {
            write_named(&mut writer, self.0).map_err(ArchiveError::from_serialize)
        }
    }

    impl<'a, T: Serialize + DeserializeOwned> IntoBytes for SerializeWrite<'a, T> {
        type ByteType = Vec<u8>;
        type RefType = &'static [u8];

        fn into_db_value(self) -> Result<Self::ByteType, ArchiveError> {
            /// The initial size to allocate when converting the result into a vector.
            const DEFAULT_SIZE: usize = usize::BITS as usize;
            let mut result = Vec::with_capacity(DEFAULT_SIZE);
            self.write(&mut result)?;
            Ok(result)
        }
    }
}
