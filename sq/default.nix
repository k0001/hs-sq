{ mkDerivation, async, async-pool, attoparsec, base, binary
, bytestring, clock, containers, contravariant, criterion, deepseq
, direct-sqlite, directory, exceptions, filepath, hedgehog, lib
, profunctors, resource-pool, resourcet, resourcet-extra
, safe-exceptions, singletons, singletons-th, stm, streaming, tasty
, tasty-hedgehog, tasty-hunit, template-haskell, text, time
, transformers
}:
mkDerivation {
  pname = "sq";
  version = "0.0.1";
  src = ./.;
  libraryHaskellDepends = [
    async attoparsec base binary bytestring clock containers
    contravariant deepseq direct-sqlite directory exceptions filepath
    profunctors resource-pool resourcet resourcet-extra safe-exceptions
    singletons singletons-th stm streaming template-haskell text time
    transformers
  ];
  testHaskellDepends = [
    base bytestring hedgehog resourcet resourcet-extra safe-exceptions
    tasty tasty-hedgehog tasty-hunit text time
  ];
  benchmarkHaskellDepends = [
    async async-pool base containers criterion resourcet
    resourcet-extra safe-exceptions stm
  ];
  homepage = "https://github.com/k0001/hs-sq";
  description = "High-level SQLite client";
  license = lib.licenses.asl20;
}
