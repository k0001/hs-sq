{ mkDerivation, adjunctions, aeson, async, async-pool, attoparsec
, attoparsec-aeson, attoparsec-iso8601, base, binary, bytestring
, clock, containers, contravariant, criterion, deepseq, df1, di
, di-core, di-df1, direct-sqlite, directory, exceptions, filepath
, foldl, hedgehog, integer-logarithms, lib, profunctors, ref-tf
, resource-pool, resourcet, resourcet-extra, safe-exceptions
, scientific, sop-core, stm, streaming, tasty, tasty-hedgehog
, tasty-hunit, template-haskell, text, time, transformers
, uuid-types
}:
mkDerivation {
  pname = "sq";
  version = "0.1";
  src = ./.;
  libraryHaskellDepends = [
    adjunctions aeson attoparsec attoparsec-aeson attoparsec-iso8601
    base binary bytestring clock containers contravariant deepseq
    di-core di-df1 direct-sqlite directory exceptions filepath foldl
    integer-logarithms profunctors ref-tf resource-pool resourcet
    resourcet-extra safe-exceptions scientific sop-core stm streaming
    template-haskell text time transformers uuid-types
  ];
  testHaskellDepends = [
    aeson async base binary bytestring df1 di di-core hedgehog ref-tf
    resourcet resourcet-extra safe-exceptions scientific sop-core tasty
    tasty-hedgehog tasty-hunit text time uuid-types
  ];
  benchmarkHaskellDepends = [
    async async-pool base containers criterion df1 di di-core resourcet
    resourcet-extra safe-exceptions stm
  ];
  homepage = "https://github.com/k0001/hs-sq";
  description = "High-level SQLite client";
  license = lib.licenses.asl20;
}
