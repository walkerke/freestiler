"""Check copied port notices and, optionally, built R/Python distributions.

Run from any directory: python3 tools/check_upstream_licenses.py [artifact ...]
No builds, installs, or network access. Artifact arguments can be R source
tarballs, Python source distributions, or wheels.
"""
import sys
import tarfile
import zipfile
from pathlib import Path


def check():
    root = Path(__file__).resolve().parents[1]
    canonical = (root / "src/rust/freestiler-core/src/supercluster/UPSTREAM-LICENSES").read_bytes()
    for relative in ("inst/COPYRIGHTS", "python/LICENSES/UPSTREAM-LICENSES"):
        if (root / relative).read_bytes() != canonical:
            raise ValueError(f"Upstream notices differ: {relative}")
    for argument in sys.argv[1:]:
        artifact = Path(argument)
        if artifact.suffix == ".whl":
            with zipfile.ZipFile(artifact) as archive:
                paths = archive.namelist()
                notices = [p for p in paths if ".dist-info/" in p and p.endswith("/UPSTREAM-LICENSES")]
                if len(notices) != 1 or archive.read(notices[0]) != canonical:
                    raise ValueError(f"Wheel lacks the full upstream notices: {artifact}")
                metadata = [p for p in paths if p.endswith(".dist-info/METADATA")]
                if len(metadata) != 1 or b"License-File: LICENSES/UPSTREAM-LICENSES" not in archive.read(metadata[0]):
                    raise ValueError(f"Wheel metadata does not declare the notices: {artifact}")
        else:
            with tarfile.open(artifact) as archive:
                paths = archive.getnames()
                is_r = any(p.endswith("/DESCRIPTION") and p.count("/") == 1 for p in paths)
                suffix = "/inst/COPYRIGHTS" if is_r else "/LICENSES/UPSTREAM-LICENSES"
                notices = [p for p in paths if p.endswith(suffix)]
                # Maturin may include the same license at both the sdist root
                # and its nested Python project. Every copy must be complete.
                if not notices or any(archive.extractfile(p).read() != canonical for p in notices):
                    raise ValueError(f"Source archive lacks the full upstream notices: {artifact}")
                if is_r and any("/freestiler-core/tests/" in p or "/supercluster-compat/" in p for p in paths):
                    raise ValueError(f"Development fixtures/harness leaked into R archive: {artifact}")
        print(f"Verified attribution: {artifact}")
    print("Canonical and R/Python notices agree.")


if __name__ == "__main__":
    check()
