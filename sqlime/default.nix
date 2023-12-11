{ mkDerivation, base, bytestring, containers, contravariant
, direct-sqlite, kan-extensions, lib, profunctors, resourcet
, safe-exceptions, stm, streaming, template-haskell, text, time
, transformers
}:
mkDerivation {
  pname = "Sqlime";
  version = "0.0.1";
  src = ./.;
  libraryHaskellDepends = [
    base bytestring containers contravariant direct-sqlite
    kan-extensions profunctors resourcet safe-exceptions stm streaming
    template-haskell text time transformers
  ];
  homepage = "https://gitlab.com/k0001/hs-sqlime";
  description = "High-level SQLite client";
  license = lib.licenses.asl20;
}
