use crate::*;
use ::bytemuck::*;

/// Archives a type by copying its raw bytes. Endianness is conserved when the value is stored.
pub struct Pod<T: ::bytemuck::Pod + Zeroable>(PhantomData<T>);

impl<T: ::bytemuck::Pod + Zeroable> DataConverter for Pod<T>
where
    [(); size_of::<T>()]:,
{
    type Target = T;
}

impl<'a, T: 'a + ::bytemuck::Pod + Zeroable> DataTransform<'a, &'a T, ToArchive> for Pod<T>
where
    [(); size_of::<T>()]:,
{
    type Output = &'a [u8; size_of::<T>()];

    fn apply(input: &'a T) -> Result<Self::Output, ArchiveError> {
        unsafe { Ok(transmute(input)) }
    }
}

impl<'a, T: ::bytemuck::Pod + Zeroable, I: 'a + AsRef<[u8; size_of::<T>()]>>
    DataTransform<'a, I, FromArchive> for Pod<T>
where
    [(); size_of::<T>()]:,
{
    type Output = PodGuard<T, I>;

    fn apply(input: I) -> Result<Self::Output, ArchiveError> {
        Ok(PodGuard(input, PhantomData))
    }
}

/// Provides a view of `Pod`-archived data in an archive.
pub struct PodGuard<T: ::bytemuck::Pod + Zeroable, I: AsRef<[u8; size_of::<T>()]>>(
    I,
    PhantomData<T>,
);

impl<T: std::fmt::Debug + ::bytemuck::Pod, I: AsRef<[u8; size_of::<T>()]>> std::fmt::Debug
    for PodGuard<T, I>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PodGuard").field(&**self).finish()
    }
}

impl<T: ::bytemuck::Pod + Zeroable, I: AsRef<[u8; size_of::<T>()]>> Deref for PodGuard<T, I> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { transmute(self.0.as_ref()) }
    }
}
