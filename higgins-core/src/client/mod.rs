
use bytes::BytesMut;


#[derive(Debug)]
pub enum ClientRef {
    AsyncTcpSocket(tokio::sync::mpsc::Sender<BytesMut>),
}

impl ClientRef {}

#[derive(Debug)]
pub struct ClientCollection(Vec<(u64, ClientRef)>);

impl ClientCollection {
    pub fn empty() -> Self {
        Self(vec![])
    }

    /// Gets an open index from a range.
    fn get_open_index(&self) -> u64 {
        // As this is sorted, we can just iterate and return the missing index.
        let index_watermark = 0;

        // let val = (0..self.0.len()).enumerate().find(|(index, val)|);

        0

        // // We just want to continue if this is true.
        // if i == 0 {
        //     continue;
        // }

        // // If it is the last index, we just return self.len().
        // if i == self. 0 .len() - 1  {
        //     return self.0.len().into();
        // }

        // // otherwise we get a value from the range between this one and the previous.
        // let current_index = self.0.get(i).unwrap().0;
        // let prev_index = self.0.get(i - 1).unwrap().0;

        // for i in prev_index..current_index {
        //     // break and return the first index.
        //     return i;
        // }

        // };
    }

    pub fn insert(&mut self, client: ClientRef) -> u64 {
        let index = self.get_open_index();

        index
    }

    pub fn get(&self, client_id: u64) -> Option<&ClientRef> {
        self.0
            .binary_search_by(|v| v.0.cmp(&client_id))
            .map(|i| self.0.get(i).map(|(_, client_ref)| client_ref))
            .ok()
            .flatten()
    }
}
