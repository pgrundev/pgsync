# Homebrew formula for pgsync. Lives in the pgrundev/homebrew-tap repo as
# Formula/pgsync.rb; this copy is the template kept next to the code.
#
# It builds from the tagged source archive, so it works on every platform Homebrew
# supports. After each release, update `url` and `sha256` (sha256 of the tag archive):
#
#   curl -sL https://github.com/pgrundev/pgsync/archive/refs/tags/vX.Y.Z.tar.gz | shasum -a 256
#
#   brew install pgrundev/tap/pgsync
class Pgsync < Formula
  desc "Memory-bounded PostgreSQL logical replication (snapshot + pgoutput CDC)"
  homepage "https://github.com/pgrundev/pgsync"
  url "https://github.com/pgrundev/pgsync/archive/refs/tags/v__VERSION__.tar.gz"
  sha256 "__SHA256__"
  license "Apache-2.0"
  head "https://github.com/pgrundev/pgsync.git", branch: "main"

  depends_on "rust" => :build
  depends_on "libpq" # pg_dump, used by `pgsync bootstrap` for the target schema

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/pgsync-cli")
    pkgshare.install "examples"
  end

  def caveats
    <<~EOS
      pgsync bootstrap runs pg_dump for the target schema. Homebrew's libpq is keg-only;
      put it on PATH or set schema.pg_dump_path:
        export PATH="#{Formula["libpq"].opt_bin}:$PATH"
      An example configuration is in #{opt_pkgshare}/examples/pgsync.yaml
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/pgsync --version")
    (testpath/"bad.yaml").write("job: {id: x}\n")
    output = shell_output("#{bin}/pgsync --config #{testpath}/bad.yaml check 2>&1", 2)
    assert_match "invalid configuration", output
  end
end
