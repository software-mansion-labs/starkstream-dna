{
  description = "Starkstream DNA development environment";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
    };
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
    };
  };

  outputs = { nixpkgs, rust-overlay, flake-utils, crane, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        crates = {
          dna-starknet = {
            description = "The Starknet DNA server";
            path = ./starknet;
            binary = "starkstream-dna-starknet";
            ports = {
              "7007/tcp" = { };
            };
          };

          benchmark = {
            description = "Starkstream DNA benchmark";
            path = ./benchmark;
            binary = "starkstream-dna-benchmark";
            ports = { };
          };
        };

        buildArtifacts = pkgs.callPackage ./nix/build.nix {
          inherit crane crates;
          workspaceDir = ./.;
        };

        ci = pkgs.callPackage ./nix/ci.nix { };

      in
      {
        # format with `nix fmt`
        formatter = pkgs.nixpkgs-fmt;

        # checks. run with `nix flake check`.
        checks = buildArtifacts.checks;

        # development shells. start with `nix develop`.
        devShells = (buildArtifacts.shell // ci.shell // { });

        # all packages.
        # show them with `nix flake show`.
        # build with `nix build .#<name>`.
        packages = (buildArtifacts.packages // { });
      }
    );

}
