# Version 0.1

* CHANGED the type of `encodeAeson` and `encodeBinary`.

* Added `hinput`, `houtput`, `encodeNS`, `decodeNS`, `encodeAeson'`,
  `encodeBinary'`.

* Added `Contravariant` `Rep` instance for `Encode` and `Input`.

* Added `InputDefault`, `OutputDefault` and related instances.

* Added `EncodeDefault` instances for `aeson`'s `Encoding and `Value`,
  and for `binary`'s `Put`.

* Export `BindingName` constructor.

# Version 0.0.2

* Added `FromJSON`, `ToJSON` instances for `Name`.

* Improved type-parameter order in `zero`, `one`, `foldM` and similar.

* Added manual transactional migrations suport.


# Version 0.0.1

* Initial version.
