#!/usr/bin/env python3
# =============================================================================
# StreamGraph — Test Suite Runner
# =============================================================================
# Runs all unit and integration tests across the project and reports summary.
#
# USAGE:
#   python tests/run_tests.py
# =============================================================================

import os
import sys
import unittest
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Auto-switch to project virtual environment if running with system Python
_venv_win = PROJECT_ROOT / ".venv" / "Scripts" / "python.exe"
_venv_nix = PROJECT_ROOT / ".venv" / "bin" / "python"
_venv_py = _venv_win if _venv_win.exists() else (_venv_nix if _venv_nix.exists() else None)

if _venv_py and Path(sys.executable).resolve() != _venv_py.resolve():
    import subprocess
    sys.exit(subprocess.call([str(_venv_py), __file__] + sys.argv[1:]))



def main():
    print("=" * 70)
    print("StreamGraph — Comprehensive Test Suite")
    print("=" * 70)

    loader = unittest.TestLoader()
    suite = loader.discover(start_dir=str(PROJECT_ROOT / "tests"), pattern="test_*.py")

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    print("=" * 70)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures:  {len(result.failures)}")
    print(f"Errors:    {len(result.errors)}")
    print(f"Skipped:   {len(result.skipped)}")
    print("=" * 70)

    if result.wasSuccessful():
        print("ALL TESTS PASSED SUCCESSFULLY! [OK]")
        sys.exit(0)
    else:
        print("TEST SUITE FAILED [FAIL]")
        sys.exit(1)


if __name__ == "__main__":
    main()
