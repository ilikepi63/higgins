//! File-related utilities for managing Subscriptions.

use higgins_shared::{PartitionName, PartitionNameError, SubscriptionError};
use std::{
    fs::OpenOptions,
    io::{Read, Seek, SeekFrom, Write},
    ops::Range,
};

#[allow(unused)]
static BODY_INDEX: usize = size_of::<u64>() * 2;

/// Represents the header for this specific file.
#[allow(unused)]
struct SubscriptionFileHeader;
#[allow(unused)]
static SPLIT_OFFSET: usize = 0;
static LEN_OFFSET: usize = size_of::<u64>();

static HEADER_SIZE: usize = LEN_OFFSET + size_of::<u64>();

/// Represents the current offset of a partition, as well as the maximum offset for that specific partition.
#[derive(Clone, Debug)]
pub struct PartitionOffsetsSerde<'a>(&'a [u8]);
static LAST_COMPLETED_OFFSET: usize = size_of::<PartitionName>();
static MAX_OFFSET: usize = LAST_COMPLETED_OFFSET + size_of::<u64>();
static AMOUNT_TO_TAKE_OFFSET: usize = MAX_OFFSET + size_of::<u64>();

// len of a serialized partition offset.
static PARTITION_OFFSET_SERDE_LEN: usize = AMOUNT_TO_TAKE_OFFSET + size_of::<u64>();

impl<'a> PartitionOffsetsSerde<'a> {
    pub fn write_to(
        partition_id: PartitionName,
        last_completed_offset: u64,
        max_offset: u64,
        amount_to_take: u64,
        dest: &mut [u8],
    ) {
        // TODO: perhaps we want to check the size of dest first?
        dest[0..LAST_COMPLETED_OFFSET].clone_from_slice(&partition_id.to_vec());
        dest[LAST_COMPLETED_OFFSET..MAX_OFFSET]
            .clone_from_slice(&last_completed_offset.to_be_bytes());
        dest[MAX_OFFSET..AMOUNT_TO_TAKE_OFFSET].clone_from_slice(&max_offset.to_be_bytes());
        dest[AMOUNT_TO_TAKE_OFFSET..AMOUNT_TO_TAKE_OFFSET + size_of::<u64>()]
            .clone_from_slice(&amount_to_take.to_be_bytes());
    }

    pub fn of(data: &'a [u8]) -> Self {
        Self(data)
    }

    pub fn get_partition_name(&self) -> Result<PartitionName, PartitionNameError> {
        let partition_name_bytes = &self.0[0..LAST_COMPLETED_OFFSET];
        PartitionName::try_from(partition_name_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionOffsetsOwned([u8; PARTITION_OFFSET_SERDE_LEN]);

impl PartitionOffsetsOwned {
    pub fn of(data: &[u8]) -> Result<Self, SubscriptionError> {
        Ok(Self(data.try_into()?))
    }
    pub fn get_partition_name(&self) -> Result<PartitionName, PartitionNameError> {
        let partition_name_bytes = &self.0[0..LAST_COMPLETED_OFFSET];
        PartitionName::try_from(partition_name_bytes)
    }
    pub fn acknowledge(&mut self, range: &Range<u64>) -> Result<(), SubscriptionError> {
        let current = u64::from_be_bytes(
            self.0[LAST_COMPLETED_OFFSET..LAST_COMPLETED_OFFSET + size_of::<u64>()].try_into()?,
        );

        // Handle if the start of this range does not equal the given offset.
        if current < range.start {
            tracing::error!(
                "Couldn't update the acknowledged outputs. Current: {current}. Range start: {}",
                range.start
            );
            return Err(
                SubscriptionError::AttemptToAcknowledgeOffsetWithoutAcknowledgingPreviousOffset(
                    current,
                    range.start,
                ),
            );
        }

        self.0[LAST_COMPLETED_OFFSET..LAST_COMPLETED_OFFSET + size_of::<u64>()]
            .copy_from_slice(&(range.end + 1).to_be_bytes());

        Ok(())
    }

    pub fn get_last_completed_offset(&self) -> Result<u64, SubscriptionError> {
        let offset = u64::from_be_bytes(
            self.0[LAST_COMPLETED_OFFSET..LAST_COMPLETED_OFFSET + size_of::<u64>()].try_into()?,
        );

        Ok(offset)
    }

    pub fn set_max_offset(&mut self, val: &u64) -> Result<(), SubscriptionError> {
        tracing::trace!("Setting max offset {val}");

        self.0[MAX_OFFSET..MAX_OFFSET + size_of::<u64>()].copy_from_slice(&val.to_be_bytes());

        Ok(())
    }

    pub fn get_max_offset(&self) -> Result<u64, SubscriptionError> {
        #[cfg(test)]
        {
            test::debug_subscription_bytes(&self.0);
        }
        let offset =
            u64::from_be_bytes(self.0[MAX_OFFSET..MAX_OFFSET + size_of::<u64>()].try_into()?);

        Ok(offset)
    }
}

pub struct SubscriptionFile {
    path: std::path::PathBuf,
}

impl SubscriptionFile {
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, SubscriptionError> {
        tracing::trace!(
            "Create subscription for file path: {:#?}",
            path.as_ref().to_str()
        );

        let mut handle = OpenOptions::new().create(true).append(true).open(&path)?;

        // nulled out as both need to be null.
        let header_buffer = [0_u8; HEADER_SIZE];

        handle.write_all(&header_buffer)?;

        let mut path_buf = std::path::PathBuf::new();
        path_buf.push(path);

        Ok(Self { path: path_buf })
    }

    pub fn add_partition(&self, partition: &PartitionName) -> Result<(), SubscriptionError> {
        let mut handle = OpenOptions::new().append(true).open(&self.path)?;

        let mut buffer = [0_u8; PARTITION_OFFSET_SERDE_LEN];

        PartitionOffsetsSerde::write_to(partition.clone(), 0, 0, 0, &mut buffer);

        handle.write_all(&buffer)?;

        handle.flush()?;

        Ok(())
    }

    /// Reads all of the partition offsets for a stream
    /// into memory.
    pub fn get_partition_indexes(
        &mut self,
    ) -> Result<Vec<PartitionOffsetsOwned>, SubscriptionError> {
        let mut buffer = [0_u8; ITER_SIZE];
        let mut handle = OpenOptions::new().read(true).open(&self.path)?;
        handle.seek(SeekFrom::Start(HEADER_SIZE as u64))?;
        let mut current_buffer_len = handle.read(&mut buffer)?;

        let mut result = vec![];

        let mut length = (current_buffer_len) / PARTITION_OFFSET_SERDE_LEN;

        while length > 0 {
            for i in 0..length {
                let current_partition_index = i * PARTITION_OFFSET_SERDE_LEN;
                let partition_bytes = &buffer
                    [current_partition_index..current_partition_index + PARTITION_OFFSET_SERDE_LEN];

                let partition = PartitionOffsetsOwned::of(partition_bytes)?;

                result.push(partition);
            }

            // if the length is more than the iter size, this means that the
            // file could be larger than our buffer size.
            if length >= ITER_SIZE {
                // Read the contents of a file, we likely only want to do this if we have exhausted the current buffer.
                handle.seek(SeekFrom::Start((HEADER_SIZE + current_buffer_len) as u64))?;
                current_buffer_len = handle.read(&mut buffer)?;
                length = (current_buffer_len) / PARTITION_OFFSET_SERDE_LEN;
            } else {
                length = 0;
            }
        }

        Ok(result)
    }

    pub fn find_index<F>(&mut self, mut predicate: F) -> Option<u64>
    where
        F: FnMut(&PartitionOffsetsSerde) -> bool,
    {
        let mut current_buffer_index = 0;
        let mut buffer = [0_u8; ITER_SIZE];
        let mut handle = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .inspect_err(|err| tracing::error!("{:#?}", err))
            .ok()?;
        handle
            .seek(SeekFrom::Start(HEADER_SIZE as u64))
            .inspect_err(|err| tracing::error!("{:#?}", err))
            .ok()?;
        let mut current_buffer_len = handle.read(&mut buffer).ok()?;

        let mut index = 0;

        // Whilst we have a buffer that has partitions inside of it.
        while current_buffer_len >= PARTITION_OFFSET_SERDE_LEN {
            if current_buffer_index >= current_buffer_len / PARTITION_OFFSET_SERDE_LEN {
                // Read the contents of a file, we likely only want to do this if we have exhausted the current buffer.
                current_buffer_len = handle.read(&mut buffer).ok()?;
                current_buffer_index = 0;
            }

            let current_partition_index = current_buffer_index * PARTITION_OFFSET_SERDE_LEN;
            let partition_bytes = &buffer
                [current_partition_index..current_partition_index + PARTITION_OFFSET_SERDE_LEN];

            let partition = PartitionOffsetsSerde::of(partition_bytes);

            let result = predicate(&partition);

            if result {
                return Some(index);
            }

            index += 1;
            current_buffer_index += 1;
        }

        None
    }

    /// Given an index, will calculate the offset used to request that partition's data
    /// from the subscription file.
    fn calculate_offset(i: u64) -> u64 {
        let offset = i * PARTITION_OFFSET_SERDE_LEN as u64;

        offset + HEADER_SIZE as u64
    }

    /// Gets the owned `PartitionOffsetsOwned` at the given index.
    pub fn get_at(&self, i: u64) -> Result<PartitionOffsetsOwned, SubscriptionError> {
        let offset = Self::calculate_offset(i);

        let mut file = OpenOptions::new().read(true).open(&self.path)?;

        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = [0_u8; PARTITION_OFFSET_SERDE_LEN];

        file.read_exact(&mut buffer)?;

        PartitionOffsetsOwned::of(&buffer)
    }

    /// Write the given buffer at the provided index.
    pub fn put_at(
        &self,
        i: u64,
        partition: PartitionOffsetsOwned,
    ) -> Result<(), SubscriptionError> {
        let offset = Self::calculate_offset(i);

        let mut file = OpenOptions::new().write(true).open(&self.path)?;

        file.seek(SeekFrom::Start(offset))?;

        file.write_all(&partition.0)?;

        file.flush()?;

        Ok(())
    }

    /// Acknowledge the given offsets for this specific file/partition.
    pub fn acknowledge(
        &mut self,
        partition_name: &PartitionName,
        offsets: &Range<u64>,
    ) -> Result<(), SubscriptionError> {
        let index = self
            .find_index(|partition| {
                partition
                    .get_partition_name()
                    .ok()
                    .map(|p_name| p_name == *partition_name)
                    .unwrap_or(false)
            })
            .ok_or(SubscriptionError::PartitionDoesNotExists)?;

        let mut partition = self.get_at(index)?;

        partition.acknowledge(offsets)?;

        self.put_at(index, partition)?;

        Ok(())
    }

    /// Increment the max offset for a partition.
    pub fn set_max_offset(
        &mut self,
        partition_name: &PartitionName,
        max_offset: &u64,
    ) -> Result<(), SubscriptionError> {
        let index = self
            .find_index(|partition| {
                partition
                    .get_partition_name()
                    .ok()
                    .map(|p_name| p_name == *partition_name)
                    .unwrap_or(false)
            })
            .ok_or(SubscriptionError::PartitionDoesNotExists)?;

        let mut partition = self.get_at(index)?;

        partition.set_max_offset(max_offset)?;

        self.put_at(index, partition)?;

        Ok(())
    }

    /// Deletes this file.
    pub fn delete(&mut self) -> Result<(), SubscriptionError> {
        std::fs::remove_file(&self.path)?;
        Ok(())
    }
}

// Holds up to 1000 partition offsets.
static PARTITION_COUNT_PER_BUFFER: usize = 1000;
static ITER_SIZE: usize = PARTITION_OFFSET_SERDE_LEN * PARTITION_COUNT_PER_BUFFER;

#[cfg(test)]
mod test {
    #![allow(clippy::unwrap_used)]
    use std::panic::catch_unwind;
    use std::{io::Read, ops::Range, path::PathBuf, str::FromStr};

    use higgins_shared::PartitionName;

    use crate::subscription::file::{
        AMOUNT_TO_TAKE_OFFSET, LAST_COMPLETED_OFFSET, MAX_OFFSET, PARTITION_OFFSET_SERDE_LEN,
        PartitionOffsetsOwned, PartitionOffsetsSerde, SubscriptionFile,
    };

    use crate::utils::test::{ByteInterval, Interval, print_bytes_coloured};
    use colored::Color;

    // static LAST_COMPLETED_OFFSET: usize = size_of::<PartitionName>();
    // static MAX_OFFSET: usize = LAST_COMPLETED_OFFSET + size_of::<u64>();
    // static AMOUNT_TO_TAKE_OFFSET: usize = MAX_OFFSET + size_of::<u64>();

    pub fn debug_subscription_bytes(b: &[u8]) {
        let intervals = &mut [
            Interval(
                ByteInterval(0, LAST_COMPLETED_OFFSET),
                Color::Blue,
                "PartitionName".to_string(),
            ),
            Interval(
                ByteInterval(LAST_COMPLETED_OFFSET, MAX_OFFSET),
                Color::Green,
                "Last completed Offset".to_string(),
            ),
            Interval(
                ByteInterval(MAX_OFFSET, AMOUNT_TO_TAKE_OFFSET),
                Color::Red,
                "Max Offset".to_string(),
            ),
            Interval(
                ByteInterval(
                    AMOUNT_TO_TAKE_OFFSET,
                    AMOUNT_TO_TAKE_OFFSET + size_of::<u64>(),
                ),
                Color::Yellow,
                "Amount to take".to_string(),
            ),
            // TODO: Probably make this dynamic?
        ];

        print_bytes_coloured(b, intervals);
    }

    #[test]
    fn can_add_partition_to_file() {
        let path = PathBuf::from_str("partition_add_test").unwrap();

        let sub_file = SubscriptionFile::new(&path).unwrap();

        ["test_one", "test_two"].iter().for_each(|name| {
            let partition_name = PartitionName::try_from(*name).unwrap();

            sub_file.add_partition(&partition_name).unwrap();
        });

        let mut buf = Vec::new();

        let mut file = std::fs::File::open(&path).unwrap();

        file.read_to_end(&mut buf).unwrap();

        let mut expected = vec![0_u8; 16];

        expected.extend_from_slice(&[
            116, 101, 115, 116, 95, 111, 110, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 116, 101, 115, 116, 95, 116, 119, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0,
        ]);

        std::fs::remove_file(&path).unwrap();

        assert_eq!(buf, expected);
    }

    fn print_file_contents<P: AsRef<std::path::Path>>(path: P) {
        let mut buf = Vec::new();

        let mut file = std::fs::File::open(&path).unwrap();

        file.read_to_end(&mut buf).unwrap();

        dbg!(buf);
    }

    #[test]
    fn iterate_subscription_file() {
        let path = PathBuf::from_str("subscription_test").unwrap();

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

        ["test_one", "test_two", "test_three", "test_four"]
            .iter()
            .for_each(|name| {
                let partition_name = PartitionName::try_from(*name).unwrap();

                sub_file.add_partition(&partition_name).unwrap();
            });

        let mut buf = Vec::new();

        let mut file = std::fs::File::open(&path).unwrap();

        file.read_to_end(&mut buf).unwrap();

        let partition = sub_file.find_index(|partition| {
            partition.get_partition_name().unwrap() == PartitionName::try_from("test_one").unwrap()
        });

        let partition = sub_file.get_at(partition.unwrap()).unwrap();

        let partition_name = partition.get_partition_name().unwrap();

        assert_eq!(partition_name, PartitionName::try_from("test_one").unwrap());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn can_get_data_at() {
        let path = PathBuf::from_str("get_at_test").unwrap();

        let sub_file = SubscriptionFile::new(&path).unwrap();

        ["test_one", "test_two", "test_three", "test_four"]
            .iter()
            .for_each(|name| {
                let partition_name = PartitionName::try_from(*name).unwrap();

                sub_file.add_partition(&partition_name).unwrap();
            });

        let partition = sub_file.get_at(0).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("test_one").unwrap()
        );

        let partition = sub_file.get_at(1).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("test_two").unwrap()
        );

        let partition = sub_file.get_at(2).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("test_three").unwrap()
        );

        let partition = sub_file.get_at(3).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("test_four").unwrap()
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn can_put_data_at() {
        let path = PathBuf::from_str("put_at_data_test").unwrap();

        let sub_file = SubscriptionFile::new(&path).unwrap();

        ["test_one", "test_two", "test_three", "test_four"]
            .iter()
            .for_each(|name| {
                let partition_name = PartitionName::try_from(*name).unwrap();

                sub_file.add_partition(&partition_name).unwrap();
            });

        let mut buffer = [0; PARTITION_OFFSET_SERDE_LEN];

        PartitionOffsetsSerde::write_to(
            PartitionName::try_from("replacement").unwrap(),
            1,
            2,
            3,
            &mut buffer,
        );

        let partition = PartitionOffsetsOwned::of(&buffer).unwrap();

        sub_file.put_at(0, partition).unwrap();

        let partition = sub_file.get_at(0).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("replacement").unwrap()
        );

        PartitionOffsetsSerde::write_to(
            PartitionName::try_from("replacement").unwrap(),
            1,
            2,
            3,
            &mut buffer,
        );

        let partition = PartitionOffsetsOwned::of(&buffer).unwrap();

        sub_file.put_at(1, partition).unwrap();

        let partition = sub_file.get_at(1).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("replacement").unwrap()
        );

        let partition = sub_file.get_at(2).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("test_three").unwrap()
        );

        let partition = sub_file.get_at(3).unwrap();
        assert_eq!(
            partition.get_partition_name().unwrap(),
            PartitionName::try_from("test_four").unwrap()
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn iterate_subscription_file_find_index() {
        let path = PathBuf::from_str("find_index_test").unwrap();

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

        print_file_contents(&path);

        ["test_one", "test_two", "test_three", "test_four"]
            .iter()
            .for_each(|name| {
                let partition_name = PartitionName::try_from(*name).unwrap();

                sub_file.add_partition(&partition_name).unwrap();
            });

        print_file_contents(&path);

        let partition = sub_file.find_index(|partition| {
            partition.get_partition_name().unwrap() == PartitionName::try_from("test_one").unwrap()
        });

        dbg!(&partition);

        assert!(matches!(partition, Some(0)));

        let partition = sub_file.find_index(|partition| {
            partition.get_partition_name().unwrap() == PartitionName::try_from("test_two").unwrap()
        });

        assert!(matches!(partition, Some(1)));

        let partition = sub_file.find_index(|partition| {
            partition.get_partition_name().unwrap()
                == PartitionName::try_from("test_three").unwrap()
        });

        dbg!(&partition);

        assert!(matches!(partition, Some(2)));

        let partition = sub_file.find_index(|partition| {
            partition.get_partition_name().unwrap() == PartitionName::try_from("test_four").unwrap()
        });

        dbg!(&partition);

        assert!(matches!(partition, Some(3)));

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn can_successfully_acknowledge_partition() {
        let path = PathBuf::from_str("acknowledge_test").unwrap();

        let result = catch_unwind(|| {
            let mut sub_file = SubscriptionFile::new(&path).unwrap();

            ["test_one", "test_two", "test_three", "test_four"]
                .iter()
                .for_each(|name| {
                    let partition_name = PartitionName::try_from(*name).unwrap();

                    sub_file.add_partition(&partition_name).unwrap();
                });

            sub_file
                .acknowledge(
                    &PartitionName::try_from("test_three").unwrap(),
                    &Range { start: 0, end: 3 },
                )
                .unwrap();

            let partition = sub_file.get_at(2).unwrap();

            assert_eq!(partition.get_last_completed_offset().unwrap(), 4);
        });

        std::fs::remove_file(&path).unwrap();

        result.unwrap();
    }

    #[test]
    fn can_successfully_set_max_position() {
        let path = PathBuf::from_str("set_max_position_test").unwrap();

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

        ["test_one", "test_two", "test_three", "test_four"]
            .iter()
            .for_each(|name| {
                let partition_name = PartitionName::try_from(*name).unwrap();

                sub_file.add_partition(&partition_name).unwrap();
            });

        sub_file
            .set_max_offset(&PartitionName::try_from("test_three").unwrap(), &5)
            .unwrap();

        let partition = sub_file.get_at(2).unwrap();

        assert_eq!(partition.get_max_offset().unwrap(), 5);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn can_read_partitions_to_memory() {
        let path = PathBuf::from_str("can_read_partitions_to_memory").unwrap();

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

        print_file_contents(&path);

        ["test_one", "test_two", "test_three", "test_four"]
            .iter()
            .for_each(|name| {
                let partition_name = PartitionName::try_from(*name).unwrap();

                sub_file.add_partition(&partition_name).unwrap();
            });

        let partitions = sub_file.get_partition_indexes().unwrap();

        assert_eq!(
            vec![
                PartitionOffsetsOwned([
                    116, 101, 115, 116, 95, 111, 110, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0
                ]),
                PartitionOffsetsOwned([
                    116, 101, 115, 116, 95, 116, 119, 111, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0
                ]),
                PartitionOffsetsOwned([
                    116, 101, 115, 116, 95, 116, 104, 114, 101, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0
                ]),
                PartitionOffsetsOwned([
                    116, 101, 115, 116, 95, 102, 111, 117, 114, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                    0, 0, 0, 0, 0, 0, 0, 0, 0
                ])
            ],
            partitions
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn can_read_partitions_to_memory_with_correctly_placed_data() {
        let path =
            PathBuf::from_str("can_read_partitions_to_memory_with_correctly_placed_data").unwrap();

        let result = catch_unwind(|| {
            let mut sub_file = SubscriptionFile::new(&path).unwrap();

            print_file_contents(&path);

            ["test_one", "test_two", "test_three", "test_four"]
                .iter()
                .enumerate()
                .for_each(|(i, name)| {
                    let partition_name = PartitionName::try_from(*name).unwrap();

                    sub_file.add_partition(&partition_name).unwrap();
                    sub_file
                        .acknowledge(
                            &partition_name,
                            &Range {
                                start: 0,
                                end: (i as u64 * 2),
                            },
                        )
                        .unwrap();
                    sub_file
                        .set_max_offset(&partition_name, &(i as u64 * 10))
                        .unwrap();
                });

            sub_file.get_partition_indexes().unwrap()
        });

        std::fs::remove_file(&path).unwrap();

        let partitions = result.unwrap();

        for (i, partition) in partitions.iter().enumerate() {
            debug_subscription_bytes(&partition.0);

            assert_eq!(
                partition.get_last_completed_offset().unwrap(),
                ((i as u64) * 2) + 1
            );
            assert_eq!(partition.get_max_offset().unwrap(), (i as u64 * 10));
        }
    }
}
