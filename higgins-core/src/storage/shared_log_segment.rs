use crate::storage::batch_coordinate::BatchCoordinate;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use riskless::{error::RisklessError, messages::ProduceRequestCollection};

static MAGIC_NUMBER: u32 = 522;
static V1_VERSION_NUMBER: u32 = 1;

pub enum SharedLogSegmentHeader {
    V1(SharedLogSegmentHeaderV1),
}

impl SharedLogSegmentHeader {
    #[allow(unused)]
    pub fn size(&self) -> usize {
        match self {
            Self::V1(_) => SharedLogSegmentHeaderV1::size(),
        }
    }
}

impl TryFrom<Bytes> for SharedLogSegmentHeader {
    type Error = RisklessError;

    fn try_from(mut value: Bytes) -> Result<Self, Self::Error> {
        let magic_number = value.try_get_u32().map_err(|err| {
            RisklessError::UnableToPassHeaderError(format!(
                "Failed to retrieve u32 from Header: {:#?}",
                err
            ))
        })?;

        if magic_number != MAGIC_NUMBER {
            return Err(RisklessError::InvalidMagicNumberError(magic_number));
        }

        let version = value.try_get_u32().map_err(|err| {
            RisklessError::UnableToPassHeaderError(format!(
                "Failed to retrieve u32 from Header: {:#?}",
                err
            ))
        })?;

        match version {
            1 => Ok(SharedLogSegmentHeader::V1(SharedLogSegmentHeaderV1)),
            _ => Err(RisklessError::InvalidSharedLogSegmentVersionNumber(version)),
        }
    }
}

pub struct SharedLogSegmentHeaderV1;

impl SharedLogSegmentHeaderV1 {
    pub const fn version_number() -> u32 {
        V1_VERSION_NUMBER
    }

    pub const fn magic_number() -> u32 {
        MAGIC_NUMBER
    }

    pub const fn size() -> usize {
        std::mem::size_of::<u32>() + std::mem::size_of::<u32>()
    }

    pub fn bytes() -> Bytes {
        let mut bytes = BytesMut::new();

        bytes.put_u32(Self::magic_number());
        bytes.put_u32(Self::version_number());

        bytes.into()
    }
}

pub struct SharedLogSegment(Vec<BatchCoordinate>, BytesMut);

// TODO: ADD IN OBJECT NAME FOR COLLECTION?
impl TryFrom<([u8; 16], ProduceRequestCollection)> for SharedLogSegment {
    type Error = RisklessError;

    fn try_from(
        (object_key, mut value): ([u8; 16], ProduceRequestCollection),
    ) -> Result<Self, Self::Error> {
        let mut buf = BytesMut::with_capacity(value.size().try_into()?);

        buf.put_slice(&SharedLogSegmentHeaderV1::bytes());

        let mut batch_coords = Vec::with_capacity(value.iter_partitions().count()); // TODO: probably just get length here?

        // TODO: probably put some file  header stuff in here..
        let base_offset = SharedLogSegmentHeaderV1::size().try_into()?;

        for partition in value.iter_partitions() {
            for req in partition.value() {
                let offset: u64 = (buf.len()).try_into()?;
                let size = req.data.len();

                buf.put_slice(&req.data);

                batch_coords.push(BatchCoordinate {
                    topic: req.topic.clone(),
                    partition: req.partition.clone(),
                    base_offset,
                    offset,
                    size: size.try_into()?,
                    request: req.clone(),
                    object_key,
                });
            }
        }

        Ok(SharedLogSegment(batch_coords, buf))
    }
}

impl SharedLogSegment {
    pub fn get_batch_coords(&self) -> Vec<BatchCoordinate> {
        self.0.clone()
    }
}

impl From<SharedLogSegment> for bytes::Bytes {
    fn from(val: SharedLogSegment) -> Self {
        val.1.into()
    }
}
