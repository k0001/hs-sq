{ mkDerivation, async, base, binary, bytestring, containers
, contravariant, direct-sqlite, lib, profunctors, resourcet
, resourcet-extra, retry, safe-exceptions, stm, streaming
, template-haskell, text, time, transformers
}:
mkDerivation {
  pname = "sqlime";
  version = "0.0.1";
  src = ./.;
  libraryHaskellDepends = [
    async base binary bytestring containers contravariant direct-sqlite
    profunctors resourcet resourcet-extra retry safe-exceptions stm
    streaming template-haskell text time transformers
  ];
  homepage = "https://github.com/k0001/hs-sqlime";
  description = "High-level SQLite client";
  license = lib.licenses.asl20;
}
