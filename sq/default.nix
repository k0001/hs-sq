{ mkDerivation, async, async-pool, attoparsec, base, binary
, bytestring, clock, containers, contravariant, criterion, deepseq
, df1, di, di-core, di-df1, direct-sqlite, directory, exceptions
, filepath, foldl, hedgehog, lib, profunctors, resource-pool
, resourcet, resourcet-extra, safe-exceptions, stm, streaming
, tasty, tasty-hedgehog, tasty-hunit, template-haskell, text, time
, transformers
}:
mkDerivation {
  pname = "sq";
  version = "0.0.1";
  src = ./.;
  libraryHaskellDepends = [
    async attoparsec base binary bytestring clock containers
    contravariant deepseq di-core di-df1 direct-sqlite directory
    exceptions filepath foldl profunctors resource-pool resourcet
    resourcet-extra safe-exceptions stm streaming template-haskell text
    time transformers
  ];
  testHaskellDepends = [
    base bytestring df1 di di-core hedgehog resourcet resourcet-extra
    safe-exceptions tasty tasty-hedgehog tasty-hunit text time
  ];
  benchmarkHaskellDepends = [
    async async-pool base containers criterion df1 di di-core resourcet
    resourcet-extra safe-exceptions stm
  ];
  homepage = "https://github.com/k0001/hs-sq";
  description = "High-level SQLite client";
  license = lib.licenses.asl20;
}
