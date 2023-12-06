use crate::zstd::private::*;
use crate::*;
use ::zstd::*;
use zstd_safe::*;

/// Archives a byte array by serializing it using the `zstd` algorithm.
pub struct Zstd<T: ?Sized, const LEVEL: u32 = 0>(PhantomData<T>);

impl<T: DataConverter + ?Sized, const LEVEL: u32> DataConverter for Zstd<T, LEVEL>
where
    Self: for<'a> DataTransform<'a, &'a T::Target, ToArchive>,
{
    type Target = T::Target;
}

impl<'a, I: 'a, T: 'a + DataTransform<'a, I, ToArchive> + ?Sized, const LEVEL: u32>
    DataTransform<'a, I, ToArchive> for Zstd<T, LEVEL>
where
    T::Output: 'a + ToWriter,
{
    type Output = ZstdWrite<T::Output, LEVEL>;

    fn apply(input: I) -> Result<Self::Output, ArchiveError> {
        Ok(ZstdWrite(T::apply(input)?))
    }
}

impl<
        'a,
        I: 'a + std::io::BufRead,
        T: 'a + DataTransform<'a, Decoder<'a, I>, FromArchive> + ?Sized,
        const LEVEL: u32,
    > DataTransform<'a, I, FromArchive> for Zstd<T, LEVEL>
{
    type Output = T::Output;

    fn apply(input: I) -> Result<Self::Output, ArchiveError> {
        T::apply(Decoder::with_buffer(input).map_err(ArchiveError::from_deserialize)?)
    }
}

impl<'a, const LEVEL: u32> DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive>
    for Zstd<[u8], LEVEL>
{
    type Output = Vec<u8>;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, ArchiveError> {
        /// The initial size to allocate when converting the result into a vector.
        const DEFAULT_SIZE: usize = usize::BITS as usize;
        let mut res = Vec::with_capacity(DEFAULT_SIZE);
        std::io::Read::read_to_end(
            &mut Decoder::with_buffer(std::io::Cursor::new(input))
                .map_err(ArchiveError::from_deserialize)?,
            &mut res,
        )
        .map_err(ArchiveError::from_deserialize)?;
        Ok(res)
    }
}

impl<
        'a,
        T: 'a
            + DataTransform<
                'a,
                Decoder<'a, std::io::Cursor<AccessGuard<'a, &'static [u8]>>>,
                FromArchive,
            >
            + ?Sized,
        const LEVEL: u32,
    > DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive> for Zstd<T, LEVEL>
{
    type Output = T::Output;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, ArchiveError> {
        T::apply(
            Decoder::with_buffer(std::io::Cursor::new(input))
                .map_err(ArchiveError::from_deserialize)?,
        )
    }
}

/// Creates a `zstd` context to use for compressing data.
fn create_default_compression_context() -> Result<CCtx<'static>, ErrorCode> {
    let mut ctx = CCtx::create();

    ctx.set_parameter(CParameter::ChecksumFlag(false))?;
    ctx.set_parameter(CParameter::ContentSizeFlag(false))?;
    ctx.set_parameter(CParameter::DictIdFlag(false))?;

    Ok(ctx)
}

/// Hides implementation details.
mod private {
    use super::*;

    /// Writes the contents of the byte data, serializing it at the same time.
    pub struct ZstdWrite<T: ToWriter, const LEVEL: u32>(pub T);

    impl<T: ToWriter, const LEVEL: u32> ToWriter for ZstdWrite<T, LEVEL> {
        fn write<W: std::io::Write>(self, writer: W) -> Result<(), ArchiveError> {
            thread_local! {
                static COMPRESSION_CONTEXT: RefCell<Option<CCtx<'static>>> = RefCell::default();
            }

            COMPRESSION_CONTEXT.with(|ref_ctx| {
                if let Ok(mut optional_ctx) = ref_ctx.try_borrow_mut() {
                    let ctx = optional_ctx.get_or_insert_with(|| {
                        create_default_compression_context()
                            .expect("Could not create compression context.")
                    });
                    ctx.reset(ResetDirective::SessionOnly)
                        .expect("Could not reset compression context.");
                    ctx.set_parameter(CParameter::CompressionLevel(LEVEL as i32))
                        .map_err(|_| {
                            ArchiveError::Serialize(
                                "Could not set compression level on context."
                                    .to_string()
                                    .into(),
                            )
                        })?;
                    self.0
                        .write(zstd::stream::Encoder::with_context(writer, ctx).auto_finish())
                } else {
                    let mut ctx = create_default_compression_context()
                        .expect("Could not create compression context.");
                    ctx.set_parameter(CParameter::CompressionLevel(LEVEL as i32))
                        .map_err(|_| {
                            ArchiveError::Serialize(
                                "Could not set compression level on context."
                                    .to_string()
                                    .into(),
                            )
                        })?;
                    self.0
                        .write(zstd::stream::Encoder::with_context(writer, &mut ctx).auto_finish())
                }
            })
        }
    }

    impl<T: ToWriter, const LEVEL: u32> IntoBytes for ZstdWrite<T, LEVEL> {
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
