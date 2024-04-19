{
  description = "Haskell 'sq' library";

  inputs = {
    #    flakety.url = "github:k0001/flakety";
    flakety.url = "git+file:///home/k/q/flakety.git";
    nixpkgs.follows = "flakety/nixpkgs";
    flake-parts.follows = "flakety/flake-parts";
    hs_resourcet-extra.url =
      "github:k0001/hs-resourcet-extra/8b05ef384b628e66c0c6742e40290cb06aaca13a";
    hs_resourcet-extra.inputs.flakety.follows = "flakety";
    hs_direct-sqlite.url =
      "github:IreneKnapp/direct-sqlite/15528503e2a53a87c50d66f52032bda5058d46f7";
    hs_direct-sqlite.flake = false;
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
                (prev.haskell.packageOverrides or (_: _: { })) (hfinal: hprev:
                  prev.lib.optionalAttrs
                  (prev.lib.versionAtLeast hprev.ghc.version "9.6") {
                    sq = hsLib.doBenchmark (hfinal.callPackage ./sq { });
                    direct-sqlite = hfinal.callCabal2nix "direct-sqlite"
                      inputs.hs_direct-sqlite { };
                    #direct-sqlite = hsLib.addExtraLibrary
                    #  (hsLib.enableCabalFlag hprev.direct-sqlite "systemlib")
                    #  final.sqlite;
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
          default = pkgs.releaseTools.aggregate {
            name = "every output from this flake";
            constituents = [
              config.packages.sq__ghc98
              config.packages.sq__ghc98.doc
              config.devShells.ghc98
            ];
          };
        };
        devShells = let
          mkShellFor = ghc:
            ghc.shellFor {
              packages = p: [ p.sq ];
              doBenchmark = true;
              withHoogle = true;
              nativeBuildInputs = [
                pkgs.cabal-install
                pkgs.cabal2nix
                pkgs.ghcid
                pkgs.sqlite-interactive
              ];
            };
        in {
          default = config.devShells.ghc98;
          ghc98 = mkShellFor pkgs.haskell.packages.ghc98;
        };
      };
    };
}
