use std::collections::BTreeMap;

#[derive(Debug)]
pub struct UniqueCollection<T>(BTreeMap<u64, T>);

impl<T> UniqueCollection<T> {
    pub fn empty() -> Self {
        Self(BTreeMap::new())
    }

    fn get_smallest_unused(&self) -> Option<u64> {
        let mut expected = 0;

        for (&id, _) in &self.0 {
            if id > expected {
                return Some(expected);
            }
            expected = id + 1;
        }

        Some(expected)
    }

    pub fn insert(&mut self, val: T) -> Option<u64> {
        let id = self.get_smallest_unused()?;

        self.0.insert(id, val);

        Some(id)
    }

    pub fn remove(&mut self, id: u64) {
        self.0.remove(&id);
    }

    pub fn get(&self, client_id: u64) -> Option<&T> {
        self.0.get(&client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy() -> Vec<u8> {
        vec![]
    }

    #[test]
    fn starts_from_zero() {
        let mut c = UniqueCollection::empty();
        assert_eq!(c.insert(dummy()).unwrap(), 0);
        assert_eq!(c.insert(dummy()).unwrap(), 1);
        assert!(c.get(0).is_some());
        assert!(c.get(1).is_some());
    }

    #[test]
    fn reuses_smallest_available_id() {
        let mut c = UniqueCollection::empty();
        c.insert(dummy()).unwrap();
        c.insert(dummy()).unwrap();
        c.insert(dummy()).unwrap();

        c.remove(1);
        assert_eq!(c.insert(dummy()).unwrap(), 1);

        c.remove(0);
        c.remove(2);
        assert_eq!(c.insert(dummy()).unwrap(), 0);
    }

    #[test]
    fn len_and_is_empty() {
        let mut c = UniqueCollection::empty();
        assert_eq!(c.0.len(), 0);

        let id = c.insert(dummy()).unwrap();
        assert_eq!(c.0.len(), 1);
        assert_eq!(id, 0);

        c.remove(id);
        assert_eq!(c.0.len(), 0);
    }

    #[test]
    fn get_returns_none_for_missing() {
        let mut c = UniqueCollection::empty();
        c.insert(dummy()).unwrap();
        assert!(c.get(0).is_some());
        assert!(c.get(42).is_none());
    }
}
