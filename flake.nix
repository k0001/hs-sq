{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?rev=46db2e09e1d3f113a13c0d7b81e2f221c63b8ce9";
    flake-parts.url = "github:hercules-ci/flake-parts";
    haskell-flake.url = "github:srid/haskell-flake";
  };
  outputs =
    inputs@{
      self,
      nixpkgs,
      flake-parts,
      ...
    }:
    flake-parts.lib.mkFlake { inherit inputs; } (
      { withSystem, ... }:
      let
        # mapListToAttrs f [a b] = {a = f a; b = f b;}
        mapListToAttrs =
          f: xs:
          builtins.listToAttrs (
            builtins.map (x: {
              name = x;
              value = f x;
            }) xs
          );
        ghcVersions = [
          "ghc984"
          "ghc9102"
          "ghc9122"
        ];
      in
      {
        systems = nixpkgs.lib.systems.flakeExposed;
        imports = [
          inputs.haskell-flake.flakeModule
        ];
        flake.haskellFlakeProjectModules = mapListToAttrs (
          ghc:
          (
            { pkgs, lib, ... }:
            withSystem pkgs.system (
              { config, ... }: config.haskellProjects.${ghc}.defaults.projectModules.output
            )
          )
        ) ghcVersions;
        perSystem =
          {
            self',
            pkgs,
            config,
            ...
          }:
          {
            haskellProjects = mapListToAttrs (ghc: {
              basePackages = pkgs.haskell.packages.${ghc};
              settings = {
                sq = {
                  benchmark = true;
                  check = true;
                  haddock = true;
                  libraryProfiling = true;
                };
              };
              packages = {
              };
              autoWire = [
                "packages"
                "checks"
                "devShells"
              ];
              devShell = {
                tools = hp: {
                  inherit (pkgs)
                    cabal2nix
                    sqlite-interactive
                    ;
                };
              };
            }) ghcVersions;
            packages.default = self'.packages.ghc9122-sq;
            packages.doc = self'.packages.ghc9122-sq.doc;
            devShells.default = self'.devShells.ghc9122;
          };
      }
    );
}
