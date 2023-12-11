{
  description = "Haskell 'sqlime' library";

  inputs = {
    nixpkgs.url =
      "github:NixOS/nixpkgs/933d7dc155096e7575d207be6fb7792bc9f34f6d";
    flake-parts.url = "github:hercules-ci/flake-parts";
  };

  outputs = inputs@{ ... }:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      flake.overlays.default = inputs.nixpkgs.lib.composeManyExtensions [
        #inputs.hs_di.overlays.default
        (final: prev:
          let
            hsLib = prev.haskell.lib;
            hsClean = drv:
              hsLib.overrideCabal drv
              (old: { src = prev.lib.sources.cleanSource old.src; });
          in {
            haskell = prev.haskell // {
              packageOverrides = prev.lib.composeExtensions
                (prev.haskell.packageOverrides or (_: _: { })) (hself: hsuper: {
                  sqlime = hself.callPackage ./sqlime { };
                  direct-sqlite = hsLib.addExtraLibrary
                    (hsLib.enableCabalFlag hsuper.direct-sqlite "systemlib")
                    final.sqlite;

                  # Extra features.
                  resourcet = hself.resourcet_1_3_0;

                  ## Requiered updates:

                  # Tests don't compile
                  lifted-base = hsLib.dontCheck hsuper.lifted-base;
                  bsb-http-chunked = hsLib.dontCheck hsuper.bsb-http-chunked;

                  # For compatibility with 'template-haskell'
                  th-abstraction = hself.th-abstraction_0_6_0_0;
                  tagged = hsuper.tagged_0_8_8;

                  # For compatibility with 'th-abstraction'
                  bifunctors = hself.bifunctors_5_6_1;
                  aeson = hself.aeson_2_2_1_0;
                  free = hself.free_5_2;

                  # For compatibility with 'aeson'
                  hpack = hself.hpack_0_36_0;
                  aeson-pretty = hself.aeson-pretty_0_8_10;
                  attoparsec-aeson = hself.attoparsec-aeson_2_2_0_1;
                  http-conduit = hself.http-conduit_2_3_8_3;

                  # For compatibility with 'time'
                  hourglass = hsLib.appendPatch hsuper.hourglass
                    (final.fetchpatch {
                      name = "hourglass-pr-56.patch";
                      url =
                        "https://github.com/vincenthz/hs-hourglass/commit/cfc2a4b01f9993b1b51432f0a95fa6730d9a558a.patch";
                      sha256 =
                        "sha256-gntZf7RkaR4qzrhjrXSC69jE44SknPDBmfs4z9rVa5Q=";
                    });

                  # For compatibility with GHC 9.8
                  doctest = hself.doctest_0_22_2;
                  hspec = hself.hspec_2_11_7;
                  hspec-core = hself.hspec-core_2_11_7;
                  hspec-discover = hself.hspec-discover_2_11_7;
                  hspec-meta = hself.hspec-meta_2_11_7;
                  semigroupoids = hself.semigroupoids_6_0_0_1;
                  hedgehog = hself.hedgehog_1_4;

                  # For compatibility with 'hedgehog' and GHC 9.8
                  tasty-hedgehog =
                    hsLib.doJailbreak hself.tasty-hedgehog_1_4_0_2;

                  # Required by 'hpack'
                  http-client-tls = hself.http-client-tls_0_3_6_3;

                  # Required by 'warp-tls'
                  tls = hself.tls_1_9_0;

                  # Required by 'warp-tls'. Tests disabled because of sandbox constraints.
                  warp = hsLib.dontCheck hself.warp_3_3_30;

                  # Required by 'hoogle'.
                  warp-tls = hself.warp-tls_3_4_3;

                  # For compatiblity with 'crypton-connection'
                  hoogle = (hsLib.appendPatch hsuper.hoogle (final.fetchpatch {
                    name = "pr-406-patch.patch";
                    url =
                      "https://github.com/ndmitchell/hoogle/commit/b535bfe53ec44f3d7529867ce8d19d272eeeec9e.patch";
                    sha256 =
                      "sha256-//mLI7KoeNUgmWEE6z5y83ObR2tKw5DEWihvPCNzK1I=";
                  })).override { connection = hself.crypton-connection; };
                });
            };
          })
      ];
      systems = [ "x86_64-linux" "i686-linux" "aarch64-linux" ];
      perSystem = { config, pkgs, system, ... }: {
        _module.args.pkgs = import inputs.nixpkgs {
          inherit system;
          overlays = [ inputs.self.overlays.default ];
        };
        packages = {
          sqlime__ghc98 = pkgs.haskell.packages.ghc98.sqlime;
          default = pkgs.releaseTools.aggregate {
            name = "every output from this flake";
            constituents = [
              config.packages.sqlime__ghc98
              config.packages.sqlime__ghc98.doc
              config.devShells.ghc98
            ];
          };
        };
        devShells = let
          mkShellFor = ghc:
            ghc.shellFor {
              packages = p: [ p.sqlime ];
              withHoogle = true;
              nativeBuildInputs =
                [ pkgs.cabal-install pkgs.cabal2nix pkgs.ghcid pkgs.sqlite ];
            };
        in {
          default = config.devShells.ghc98;
          ghc98 = mkShellFor pkgs.haskell.packages.ghc98;
        };
      };
    };
}
