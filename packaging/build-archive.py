#!/usr/bin/env python3
"""Build and check a native release archive from already compiled binaries."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parent.parent
BINARIES = ("ftw", "ftwdb-shadow", "ftwdb-shadow-reconcile")
DOCUMENTS = (
    "README.md", "CHANGELOG.md", "LICENSE", "docs/operations.md", "docs/format.md",
    "docs/shadow-sidecar.md", "docs/releases.md", "packaging/README.md",
    "packaging/systemd/ftwdb-shadow.service",
    "packaging/launchd/com.sourceful.ftwdb-shadow.plist",
    "testdata/shadow-protocol-v1/SHA256SUMS",
)


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--require-clean", action="store_true")
    args = parser.parse_args()
    package = json.loads(output("cargo", "metadata", "--no-deps", "--format-version", "1", "--locked", "--offline"))
    version = next(p["version"] for p in package["packages"] if p["name"] == "ftwdb")
    target = next(line.removeprefix("host: ") for line in output("rustc", "-vV").splitlines() if line.startswith("host: "))
    dirty = bool(output("git", "status", "--porcelain"))
    if args.require_clean and dirty:
        raise SystemExit("release archive requires a clean source tree")
    name = f"ftw-v{version}-{target}"
    dist = ROOT / "dist"
    dist.mkdir(exist_ok=True)
    archive = dist / f"{name}.tar.gz"
    with tempfile.TemporaryDirectory(prefix="ftwdb-archive-") as temporary:
        stage = Path(temporary) / name
        stage.mkdir()
        for binary in BINARIES:
            shutil.copy2(ROOT / "target" / "release" / binary, stage / binary)
        for document in DOCUMENTS:
            destination = stage / document
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / document, destination)
        metadata = {
            "schema": "ftwdb-release-v1", "version": version, "target": target,
            "source_commit": output("git", "rev-parse", "HEAD"), "source_dirty": dirty,
            "binaries": {binary: sha256(stage / binary) for binary in BINARIES},
        }
        (stage / "SOURCE.json").write_text(json.dumps(metadata, indent=2) + "\n")
        with tarfile.open(archive, "w:gz") as bundle:
            bundle.add(stage, arcname=name)
        # Run the files from the actual archive, including both shadow tools.
        extracted = Path(temporary) / "checked"
        with tarfile.open(archive) as bundle:
            for member in bundle:
                relative = Path(member.name)
                if relative.is_absolute() or ".." in relative.parts:
                    raise SystemExit("unsafe archive path")
                if member.isdir():
                    continue
                if not member.isfile():
                    raise SystemExit("archive must contain only regular files")
                destination = extracted / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                with bundle.extractfile(member) as source, destination.open("wb") as target_file:
                    shutil.copyfileobj(source, target_file)
                destination.chmod(0o755 if relative.name in BINARIES else 0o644)
        for binary in BINARIES:
            path = extracted / name / binary
            if sha256(path) != metadata["binaries"][binary]:
                raise SystemExit(f"archive checksum mismatch: {binary}")
            if output(str(path), "--version") != f"{binary} {version}":
                raise SystemExit(f"archive version mismatch: {binary}")
    digest = sha256(archive)
    (dist / f"{archive.name}.sha256").write_text(f"{digest}  {archive.name}\n")
    print(archive)


if __name__ == "__main__":
    main()
