{
  description = "Haskell 'sq' library";

  inputs = {
    flakety.url = "github:k0001/flakety";
    nixpkgs.follows = "flakety/nixpkgs";
    flake-parts.follows = "flakety/flake-parts";
    hs_resourcet-extra.url = "github:k0001/hs-resourcet-extra";
    hs_resourcet-extra.inputs.flakety.follows = "flakety";
  };

  outputs = inputs@{ ... }:
    inputs.flake-parts.lib.mkFlake { inherit inputs; } {
      flake.overlays.default = inputs.nixpkgs.lib.composeManyExtensions [
        inputs.flakety.overlays.default
        inputs.hs_resourcet-extra.overlays.default
        (final: prev:
          let
            hsLib = prev.haskell.lib;
            hsClean = drv:
              hsLib.overrideCabal drv
              (old: { src = prev.lib.sources.cleanSource old.src; });
          in {
            haskell = prev.haskell // {
              packageOverrides = prev.lib.composeExtensions
                (prev.haskell.packageOverrides or (_: _: { })) (hself: hsuper:
                  prev.lib.optionalAttrs
                  (prev.lib.versionAtLeast hsuper.ghc.version "9.6") {
                    sq = hself.callPackage ./sq { };
                    direct-sqlite = hsLib.addExtraLibrary
                      (hsLib.enableCabalFlag hsuper.direct-sqlite "systemlib")
                      final.sqlite;
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
          sq__ghc98 = pkgs.haskell.packages.ghc98.sq;
          sq__ghc96 = pkgs.haskell.packages.ghc96.sq;
          default = pkgs.releaseTools.aggregate {
            name = "every output from this flake";
            constituents = [
              config.packages.sq__ghc98
              config.packages.sq__ghc98.doc
              config.devShells.ghc98
              config.packages.sq__ghc96
              config.packages.sq__ghc96.doc
              config.devShells.ghc96
            ];
          };
        };
        devShells = let
          mkShellFor = ghc:
            ghc.shellFor {
              packages = p: [ p.sq ];
              withHoogle = true;
              nativeBuildInputs =
                [ pkgs.cabal-install pkgs.cabal2nix pkgs.ghcid pkgs.sqlite ];
            };
        in {
          default = config.devShells.ghc98;
          ghc98 = mkShellFor pkgs.haskell.packages.ghc98;
          ghc96 = mkShellFor pkgs.haskell.packages.ghc96;
        };
      };
    };
}
