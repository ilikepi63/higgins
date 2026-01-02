//! File-related utilites for managing Subscriptions.

use higgins_shared::{PartitionName, PartitionNameError};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

static BODY_INDEX: usize = size_of::<u64>() * 2;

/// Represents the header for this specific file.
struct SubscriptionFileHeader;
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
        dest[0..LAST_COMPLETED_OFFSET].clone_from_slice(&partition_id.0);
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

#[derive(Clone, Debug)]
pub struct PartitionOffsetsOwned([u8; PARTITION_OFFSET_SERDE_LEN]);

impl PartitionOffsetsOwned {
    pub fn of(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self(data.try_into()?))
    }
    pub fn get_partition_name(&self) -> Result<PartitionName, PartitionNameError> {
        let partition_name_bytes = &self.0[0..LAST_COMPLETED_OFFSET];
        PartitionName::try_from(partition_name_bytes)
    }
}

struct SubscriptionFileTail {}

pub struct SubscriptionFile<P: AsRef<std::path::Path>> {
    path: P,
}

impl<P: AsRef<std::path::Path>> SubscriptionFile<P> {
    pub fn new(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let mut handle = OpenOptions::new().create(true).append(true).open(&path)?;

        // nulled out as both need to be null.
        let header_buffer = [0_u8; HEADER_SIZE];

        handle.write(&header_buffer)?;

        Ok(Self { path })
    }

    pub fn add_partition(
        &self,
        partition: &PartitionName,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut handle = OpenOptions::new().append(true).open(&self.path)?;

        let mut buffer = [0_u8; PARTITION_OFFSET_SERDE_LEN];

        PartitionOffsetsSerde::write_to(partition.clone(), 0, 0, 0, &mut buffer);

        println!("Writing buffer: {:#?}", buffer);

        handle.write(&mut buffer)?;

        handle.flush()?;

        Ok(())
    }

    pub fn find_index<F>(&mut self, mut predicate: F) -> Option<u64>
    where
        F: FnMut(&PartitionOffsetsSerde) -> bool,
    {
        let mut current_buffer_index = 0;
        let mut buffer = [0_u8; ITER_SIZE];
        let mut handle = OpenOptions::new().read(true).open(&self.path).unwrap();
        handle.seek(SeekFrom::Start(HEADER_SIZE as u64)).unwrap();
        let mut current_buffer_len = handle.read(&mut buffer).ok()?;
        println!("Read {current_buffer_len} bytes from file.");

        // dbg!(&buffer);

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
    pub fn get_at(&self, i: u64) -> Result<PartitionOffsetsOwned, Box<dyn std::error::Error>> {
        let offset = Self::calculate_offset(i);

        let mut file = OpenOptions::new().read(true).open(&self.path)?;

        file.seek(SeekFrom::Start(offset))?;

        let mut buffer = [0_u8; PARTITION_OFFSET_SERDE_LEN];

        file.read(&mut buffer)?;

        Ok(PartitionOffsetsOwned::of(&buffer)?)
    }

    /// Write the given buffer at the provided index.
    pub fn put_at(
        &self,
        i: u64,
        partition: PartitionOffsetsOwned,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let offset = Self::calculate_offset(i);

        let mut file = OpenOptions::new().write(true).open(&self.path)?;

        file.seek(SeekFrom::Start(offset))?;

        file.write(&partition.0)?;

        file.flush();

        Ok(())
    }

    /// Acknowledge the given offsets for this specific file/partition.
    pub fn acknowledge(&mut self, partition: &PartitionName, offsets: &[u64]) {
        let file = OpenOptions::new().write(true).open(&self.path);

        // let header =
        // Read the header for where the indexes are.

        // Iterate through the body, finding this partition.
        // acknowledge the given offsets.
        // If the current partition is converted from readable to unreadable, swap to an unreadable destination.
    }

    /// Increment the max offset for a partition.
    pub fn set_max_offset(&self, partition: &PartitionName, max_offset: &[u64]) {
        // Read the header file for the indexes.
        // Iterate through the body, finding the partition we need to Adjust.
        // Set the max_offset of this partition.
        // It will then have become readable, so if it has changed from readable to unreadable, we swap it with a readable value
    }
}

// Holds up to 1000 partition offsets.
static PARTITION_COUNT_PER_BUFFER: usize = 1000;
static ITER_SIZE: usize = PARTITION_OFFSET_SERDE_LEN * PARTITION_COUNT_PER_BUFFER;

#[cfg(test)]
mod test {
    use std::{io::Read, path::PathBuf, str::FromStr};

    use higgins_shared::PartitionName;

    use crate::subscription::file::{
        PARTITION_OFFSET_SERDE_LEN, PartitionOffsetsOwned, PartitionOffsetsSerde, SubscriptionFile,
    };

    #[test]
    fn can_add_partition_to_file() {
        let path = PathBuf::from_str("partition_add_test").unwrap();

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

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

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

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

        let mut sub_file = SubscriptionFile::new(&path).unwrap();

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

        std::fs::remove_file(&path).unwrap();

        panic!();

        assert!(matches!(partition, Some(0)));

        let partition = sub_file.find_index(|partition| {
            partition.get_partition_name().unwrap() == PartitionName::try_from("test_two").unwrap()
        });

        dbg!(&partition);

        assert!(matches!(partition, Some(1)));

        // let partition = sub_file.find_index(|partition| {
        //     partition.get_partition_name().unwrap()
        //         == PartitionName::try_from("test_three").unwrap()
        // });

        // dbg!(&partition);

        // assert!(matches!(partition, Some(2)));

        // let partition = sub_file.find_index(|partition| {
        //     partition.get_partition_name().unwrap() == PartitionName::try_from("test_four").unwrap()
        // });

        // dbg!(&partition);

        // assert!(matches!(partition, Some(3)));
    }
}
