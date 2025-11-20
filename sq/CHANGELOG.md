# Version 0.1

* CHANGED the type of `encodeAeson` and `encodeBinary`.

* REMOVED `encodeSizedIntegral`. Go through `Int64` or `Scientific` instead.

* REMOVED `decodeSizedIntegral`. Use `decodeBoundedIntegral` instead.

* Added `hinput`, `houtput`, `encodeNS`, `decodeNS`, `encodeAeson'`.

* Added `Contravariant` `Rep` instance for `Encode` and `Input`.

* Added `InputDefault`, `OutputDefault` and related instances.

* Added `EncodeDefault` instances for `aeson`'s `Encoding and `Value`,
  for `binary`'s `Put`, for `UUID`, for `Scientific`, for `Fixed`.

* Added `DecodeDefault` instances for `UUID`, for `aeson`'s `Value`, for
  `Scientific`, for `Fixed`.

* Export `BindingName` constructor.

* Export `ErrTransaction`.

* Improved asynchronous exception handling.

* Faster `ZonedTime`, `UTCTime`, `LocalTime`, `TimeZone`, `Day` and `TimeOfDay`
  parsing via `attoparsec-iso8601`


# Version 0.0.2

* Added `FromJSON`, `ToJSON` instances for `Name`.

* Improved type-parameter order in `zero`, `one`, `foldM` and similar.

* Added manual transactional migrations suport.


# Version 0.0.1

* Initial version.
