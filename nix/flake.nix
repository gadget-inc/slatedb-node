{
  description = "slatedb-node development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    (flake-utils.lib.eachDefaultSystem (system: nixpkgs.lib.fix (flake:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        packages = {
          corepack = pkgs.corepack;
          direnv = pkgs.direnv;
          git = pkgs.git;
          mkcert = pkgs.mkcert;
          nix-direnv = pkgs.nix-direnv;
          nixpkgs-fmt = pkgs.nixpkgs-fmt;
          nodejs = pkgs.nodejs_24;
        };

        devShell = pkgs.mkShell {
          packages = builtins.attrValues flake.packages;
        };
      }
    )));

  nixConfig.bash-prompt = "\[slatedb-node-develop:\\w\]$ ";
}
