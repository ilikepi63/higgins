//! The utilities surrounding mapping of joined properties to their ultime representation inside of the
//! joined dataset.

use std::collections::BTreeMap;

use crate::error::HigginsError;

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
pub struct JoinMapping(Vec<JoinMappingDerivativeToProperty>);

type JoinMappingDerivativeToProperty = (String, Vec<(String, String)>);

/// Conversion from a BTreeMap representing the Property Mapping here.
///
/// See 'ConfigurationStreamDefinition' for more information.
impl From<BTreeMap<String, String>> for JoinMapping {
    fn from(value: BTreeMap<String, String>) -> Self {
        JoinMapping(
            value
                .iter()
                .filter_map(|(resultant_name, origin)| {
                    let mut split_origin = origin.split(".");

                    if let (Some(origin), Some(origin_key)) =
                        (split_origin.next(), split_origin.next())
                    {
                        Some((resultant_name, origin, origin_key))
                    } else {
                        None
                    }
                })
                .fold(
                    Vec::<JoinMappingDerivativeToProperty>::new(),
                    |mut acc, (resultant_name, origin, origin_key)| {
                        // Return the key if the origins match.
                        let key = acc.iter_mut().find(|val| val.0 == origin);

                        if let Some(key) = key {
                            key.1.push((origin_key.to_owned(), resultant_name.clone()));
                            acc
                        } else {
                            acc.push((
                                origin.to_owned(),
                                vec![(origin_key.to_owned(), resultant_name.to_owned())],
                            ));
                            acc
                        }
                    },
                ),
        )
    }
}
