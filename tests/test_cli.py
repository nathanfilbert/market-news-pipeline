from typer.testing import CliRunner

from mnp import __version__
from mnp.cli import app


def test_version():
    result = CliRunner().invoke(app, ["version"])
    assert result.exit_code == 0
    assert result.stdout.strip() == __version__
