{
  description = "slatedb-node development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    # nixos-20.09 provides glibc 2.31 for Linux binary compatibility
    nixpkgs-glibc-2_31 = {
      url = "github:NixOS/nixpkgs/nixos-20.09";
      flake = false;
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, nixpkgs-glibc-2_31, rust-overlay, flake-utils }:
    (flake-utils.lib.eachDefaultSystem (system: nixpkgs.lib.fix (flake:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          targets = [
            "x86_64-unknown-linux-gnu"
            "x86_64-unknown-linux-musl"
            "aarch64-unknown-linux-gnu"
            "aarch64-unknown-linux-musl"
          ];
        };
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
          rust = rustToolchain;
          zig = pkgs.zig;
        };

        devShell =
          let
            mkShell =
              if pkgs.stdenv.isLinux then
                let
                  pkgsGlibc231 = import nixpkgs-glibc-2_31 { inherit system; };
                  customStdenv = pkgs.overrideCC pkgs.stdenv (pkgs.wrapCCWith {
                    cc = pkgs.gcc.cc;
                    libc = pkgsGlibc231.glibc;
                  });
                in
                pkgs.mkShell.override { stdenv = customStdenv; }
              else
                pkgs.mkShell;
          in
          mkShell {
            packages = builtins.attrValues flake.packages;
          };
      }
    )));

  nixConfig.bash-prompt = "\[slatedb-node-develop:\\w\]$ ";
}
