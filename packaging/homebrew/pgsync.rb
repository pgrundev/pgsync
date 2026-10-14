# Homebrew formula for pgsync. Lives in the pgrundev/homebrew-tap repo as
# Formula/pgsync.rb; this copy is the template kept next to the code.
#
# After each release, update `version` and the four sha256 values from the
# release's checksums.txt, then copy to the tap and push.
#
#   brew install pgrundev/tap/pgsync
class Pgsync < Formula
  desc "Memory-bounded PostgreSQL logical replication (snapshot + pgoutput CDC)"
  homepage "https://github.com/pgrundev/pgsync"
  version "0.1.1"
  license "Apache-2.0"

  base = "https://github.com/pgrundev/pgsync/releases/download/v#{version}"

  on_macos do
    on_arm do
      url "#{base}/pgsync_#{version}_darwin_arm64.tar.gz"
      sha256 "677f39cfcdc6a4ec4132e0e23225c154ad366a91142812a29431bd212fdece98"
    end
    on_intel do
      url "#{base}/pgsync_#{version}_darwin_amd64.tar.gz"
      sha256 "26bca1e2b9d262fa49cc3412f011ebb3a3fca464ded6cdbb517afeee85b33f71"
    end
  end

  on_linux do
    on_arm do
      url "#{base}/pgsync_#{version}_linux_arm64.tar.gz"
      sha256 "57aa4180069f667358108e9ac1a35ebee847e6805bafb11ae04f798f231d397c"
    end
    on_intel do
      url "#{base}/pgsync_#{version}_linux_amd64.tar.gz"
      sha256 "47ba6a00976eb704a3ac8bde416ee9992cb4dc1d8dcd284b04df3654effea127"
    end
  end

  def install
    bin.install "pgsync"
    pkgshare.install "examples"
  end

  def caveats
    <<~EOS
      pgsync bootstrap runs pg_dump (PostgreSQL client tools) for the target schema.
      If it is not on PATH:  brew install libpq && export PATH="$(brew --prefix libpq)/bin:$PATH"
      or set schema.pg_dump_path in the job config.
      Example configuration: #{opt_pkgshare}/examples/pgsync.yaml
    EOS
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/pgsync --version")
    (testpath/"bad.yaml").write("job: {id: x}\n")
    output = shell_output("#{bin}/pgsync --config #{testpath}/bad.yaml check 2>&1", 2)
    assert_match "invalid configuration", output
  end
end
