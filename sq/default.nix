{ mkDerivation, async, base, binary, bytestring, containers
, contravariant, direct-sqlite, lib, profunctors, resource-pool
, resourcet, resourcet-extra, retry, safe-exceptions, stm
, streaming, template-haskell, text, time, transformers
}:
mkDerivation {
  pname = "sq";
  version = "0.0.1";
  src = ./.;
  libraryHaskellDepends = [
    async base binary bytestring containers contravariant direct-sqlite
    profunctors resource-pool resourcet resourcet-extra retry
    safe-exceptions stm streaming template-haskell text time
    transformers
  ];
  homepage = "https://github.com/k0001/hs-sq";
  description = "High-level SQLite client";
  license = lib.licenses.asl20;
}
