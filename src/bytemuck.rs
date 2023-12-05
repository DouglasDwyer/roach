use ::bytemuck::*;
use crate::*;
use std::marker::*;
use std::mem::*;

pub struct Pod<T: ::bytemuck::Pod + Zeroable>(PhantomData<T>);

impl<T: ::bytemuck::Pod + Zeroable> DataConverter for Pod<T> where [(); size_of::<T>()]: {
    type Target = T;
}

impl<'a, T: 'a + ::bytemuck::Pod + Zeroable> DataTransform<'a, &'a T, ToArchive> for Pod<T> where [(); size_of::<T>()]: {
    type Output = &'a [u8; size_of::<T>()];

    fn apply(input: &'a T) -> Result<Self::Output, DbError> {
        unsafe {
            Ok(transmute(input))
        }
    }
}

impl<'a, T: ::bytemuck::Pod + Zeroable, I: 'a + AsRef<[u8; size_of::<T>()]>> DataTransform<'a, I, FromArchive> for Pod<T> where [(); size_of::<T>()]: {
    type Output = &'a T;

    fn apply(input: I) -> Result<Self::Output, DbError> {
        //try_from_bytes::<T>(input.as_ref()).map_err(|x| DbError::Deserialize(Box::new(x)))
        unsafe {
            Ok(transmute(input.as_ref()))
        }
    }
}