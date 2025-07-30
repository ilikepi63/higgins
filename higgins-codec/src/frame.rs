use crate::error::HigginsCodecError;

pub struct Frame(Vec<u8>);

impl Frame {
    /// Wrap an existing Vec with a Frame.
    pub fn new(buf: Vec<u8>) -> Self {
        Self(buf)
    }

    /// Try read a frame from the given Read implementation.
    pub fn try_read<R: std::io::Read>(r: &mut R) -> Result<Self, HigginsCodecError> {
        let mut buf = [0_u8; 4];

        r.read_exact(&mut buf)?;

        let size: usize = u32::from_be_bytes(buf).try_into()?;

        let mut buf = vec![0_u8; size];

        r.read_exact(&mut buf)?;

        Ok(Self(buf))
    }

    /// Try read a frame from the given AsyncRead implementation.
    pub async fn try_read_async<R: tokio::io::AsyncReadExt + std::marker::Unpin>(
        r: &mut R,
    ) -> Result<Self, HigginsCodecError> {
        let mut buf = [0_u8; 4];

        r.read_exact(&mut buf).await?;

        let size: usize = u32::from_be_bytes(buf).try_into()?;

        let mut buf = vec![0_u8; size];

        r.read_exact(&mut buf).await?;

        Ok(Self(buf))
    }

    /// Try write a frame to the given Write implementation.
    pub fn try_write<W: std::io::Write>(self, w: &mut W) -> Result<(), HigginsCodecError> {
        let size: u32 = self.0.len().try_into()?;

        w.write_all(&size.to_be_bytes())?;

        w.write_all(&self.0)?;

        Ok(())
    }

    /// Try write a frame to the given AsyncWrite implementation.
    pub async fn try_write_async<W: tokio::io::AsyncWriteExt + std::marker::Unpin>(
        self,
        w: &mut W,
    ) -> Result<(), HigginsCodecError> {
        let size: u32 = self.0.len().try_into()?;

        w.write_all(&size.to_be_bytes()).await?;

        w.write_all(&self.0).await?;

        Ok(())
    }
}
