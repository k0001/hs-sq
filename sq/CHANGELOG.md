# Version 0.0.3

* CHANGED the type of `encodeAeson`. It takes `Either`
  an `aeson` `Encoding` or a `Value` now.

* Added `hinput`, `houtput`, `encodeNS`, `decodeNS`.

* Added `Contravariant` `Rep` instance for `Encode` and `Input`.

* Added `InputDefault`, `OutputDefault`.

* Added `EncodeDefault` instances for `aeson`'s `Encoding and `Value`.

# Version 0.0.2

* Added `FromJSON`, `ToJSON` instances for `Name`.

* Improved type-parameter order in `zero`, `one`, `foldM` and similar.

* Added manual transactional migrations suport.


# Version 0.0.1

* Initial version.
