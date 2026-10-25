import subprocess
import sys

import pytest

from talo.client import TaloError, call


@pytest.fixture
def talo_home(tmp_path, monkeypatch):
    """A private TALO_HOME with its own supervisor, stopped after the test."""
    home = tmp_path / "talo"
    monkeypatch.setenv("TALO_HOME", str(home))
    yield home
    try:
        call("shutdown", autostart=False)
    except TaloError:
        pass


@pytest.fixture
def talo(talo_home):
    """Run the CLI in a subprocess: talo("run", "x = 1") -> CompletedProcess (text)."""
    def run(*args, stdin=None, timeout=120):
        return subprocess.run([sys.executable, "-m", "talo.cli", *args], input=stdin, capture_output=True, text=True,
                              timeout=timeout)
    return run
