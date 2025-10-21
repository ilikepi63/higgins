//! The utilities surrounding mapping of joined properties to their ultime representation inside of the
//! joined dataset.

use std::collections::BTreeMap;

use arrow::ipc::RecordBatch;

/// JoinMapping is the mapping metadata between a joined data structs properties
/// and its derivative properties.
///
/// For example:
///
/// Customer {
///  id,
///  first_name,
///  last_name
///}
///
/// Address {
///  customer_id,
///  address
///  etc...
///}
///
/// JoinedCustomerAddress {
///    customer_first_name: customer.first_name,
///    customer_last_name: customer.last_name,
///    customer_address: address.address
/// }
#[derive(Clone)]
pub struct JoinMapping(
    arrow::datatypes::Schema,
    Vec<JoinMappingDerivativeToProperty>,
);

type JoinMappingDerivativeToProperty = (
    StreamName,
    Vec<(StreamPropertyKey, JoinedStreamPropertyKey)>,
);

/// The original stream that this derived value comes from.
type StreamName = String;

/// The name of the key that this value originates from.
type StreamPropertyKey = String;

/// The renamed property where this value will be present in the resultant joined data structure.
type JoinedStreamPropertyKey = String;

impl JoinMapping {
    /// Given a list of record batches with their stream names,
    /// return a batch that represents the amalgamated result using this mapping.
    pub fn map_arrow(&self, batches: Vec<(String, RecordBatch)>) -> RecordBatch {
        for (stream_name, properties) in self.0.iter() {
            let (name, batch) = batches
                .iter()
                .find(|(name, _)| stream_name == name)
                .unwrap();

            for (property, joined_property) in properties.iter() {}
        }

        RecordBatch::from()
    }
}

/// Conversion from a BTreeMap representing the Property Mapping here.
///
/// See 'ConfigurationStreamDefinition' for more information.
// impl From<BTreeMap<String, String>> for JoinMapping {
//     fn from(value: BTreeMap<String, String>) -> Self {
//         JoinMapping(
//             value
//                 .iter()
//                 .filter_map(|(resultant_name, origin)| {
//                     let mut split_origin = origin.split(".");

//                     if let (Some(origin), Some(origin_key)) =
//                         (split_origin.next(), split_origin.next())
//                     {
//                         Some((resultant_name, origin, origin_key))
//                     } else {
//                         None
//                     }
//                 })
//                 .fold(
//                     Vec::<JoinMappingDerivativeToProperty>::new(),
//                     |mut acc, (resultant_name, origin, origin_key)| {
//                         // Return the key if the origins match.
//                         let key = acc.iter_mut().find(|val| val.0 == origin);

//                         if let Some(key) = key {
//                             key.1.push((origin_key.to_owned(), resultant_name.clone()));
//                             acc
//                         } else {
//                             acc.push((
//                                 origin.to_owned(),
//                                 vec![(origin_key.to_owned(), resultant_name.to_owned())],
//                             ));
//                             acc
//                         }
//                     },
//                 ),
//         )
//     }
// }
