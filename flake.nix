{
  description = "lab3-dmfrpro";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config = {
            allowUnfree = true;
            permittedInsecurePackages = [
              "openjdk-8u472-b08"
              "openjdk-8u472-b08-jre"
            ];
          };
        };
      in
      {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [
            javaPackages.compiler.openjdk8
            maven
            docker
            docker-compose
          ];

          JAVA_HOME = "${pkgs.javaPackages.compiler.openjdk8.home}";
          MAVEN_OPTS = "-Dmaven.wagon.http.ssl.insecure=true -Dmaven.wagon.http.ssl.allowall=true";
        };
      });
}
