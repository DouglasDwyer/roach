use crate::*;
use crate::zstd::private::*;
use std::cell::*;
use std::marker::*;
use ::zstd::*;
use zstd_safe::*;

pub struct Zstd<T, const LEVEL: u32 = 0>(PhantomData<T>);

impl<T: DataConverter, const LEVEL: u32> DataConverter for Zstd<T, LEVEL> where Self: for<'a> DataTransform<'a, &'a T::Target, ToArchive> {
    type Target = T::Target;
}

impl<'a, I: 'a, T: 'a + DataTransform<'a, I, ToArchive>, const LEVEL: u32> DataTransform<'a, I, ToArchive> for Zstd<T, LEVEL>
where T::Output: 'a + ToWriter
{
    type Output = ZstdWrite<T::Output, LEVEL>;

    fn apply(input: I) -> Result<Self::Output, DbError> {
        Ok(ZstdWrite(T::apply(input)?))
    }
}

impl<'a, I: 'a + std::io::BufRead, T: 'a + DataTransform<'a, Decoder<'a, I>, FromArchive>, const LEVEL: u32> DataTransform<'a, I, FromArchive> for Zstd<T, LEVEL> {
    type Output = T::Output;

    fn apply(input: I) -> Result<Self::Output, DbError> {
        T::apply(Decoder::with_buffer(input).map_err(DbError::from_deserialize)?)
    }
}

impl<'a, T: 'a + DataTransform<'a, Decoder<'a, std::io::Cursor<AccessGuard<'a, &'static [u8]>>>, FromArchive>, const LEVEL: u32> DataTransform<'a, AccessGuard<'a, &'static [u8]>, FromArchive> for Zstd<T, LEVEL> {
    type Output = T::Output;

    fn apply(input: AccessGuard<'a, &'static [u8]>) -> Result<Self::Output, DbError> {
        T::apply(Decoder::with_buffer(std::io::Cursor::new(input)).map_err(DbError::from_deserialize)?)
    }
}

/// Creates a `zstd` context to use for compressing data.
fn create_default_compression_context() -> Result<CCtx<'static>, ErrorCode> {
    let mut ctx = CCtx::create();

    ctx.set_parameter(CParameter::ChecksumFlag(false))?;
    ctx.set_parameter(CParameter::ContentSizeFlag(false))?;
    ctx.set_parameter(CParameter::DictIdFlag(false))?;
    ctx.set_parameter(CParameter::Format(FrameFormat::Magicless))?;

    Ok(ctx)
}

mod private {
    use super::*;

    pub struct ZstdWrite<T: ToWriter, const LEVEL: u32>(pub T);

    impl<T: ToWriter, const LEVEL: u32> ToWriter for ZstdWrite<T, LEVEL> {
        fn write<W: std::io::Write>(self, writer: W) -> Result<(), DbError> {
            thread_local! {
                static COMPRESSION_CONTEXT: RefCell<Option<CCtx<'static>>> = RefCell::default();
            }

            COMPRESSION_CONTEXT.with(|ref_ctx| {
                if let Ok(mut optional_ctx) = ref_ctx.try_borrow_mut() {
                    let ctx = optional_ctx.get_or_insert_with(|| create_default_compression_context().expect("Could not create compression context."));
                    ctx.reset(ResetDirective::SessionOnly).expect("Could not reset compression context.");
                    ctx.set_parameter(CParameter::CompressionLevel(LEVEL as i32)).map_err(|_| DbError::Serialize(format!("Could not set compression level on context.").into()))?;
                    self.0.write(zstd::stream::Encoder::with_context(writer, ctx))
                }
                else {
                    let mut ctx = create_default_compression_context().expect("Could not create compression context.");
                    ctx.set_parameter(CParameter::CompressionLevel(LEVEL as i32)).map_err(|_| DbError::Serialize(format!("Could not set compression level on context.").into()))?;
                    self.0.write(zstd::stream::Encoder::with_context(writer, &mut ctx))
                }
            })
        }
    }
}