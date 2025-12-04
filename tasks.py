#!/usr/bin/env python
"""Task runner for submitthem project - cross-platform alternative to Makefile."""

import os
import shutil
import subprocess
import sys
from pathlib import Path

# Configuration
USE_VENV = os.getenv("USE_VENV", "1") == "1"
VENV_DIR = Path("venv")
BIN_DIR = VENV_DIR / "bin" if not sys.platform.startswith("win") else VENV_DIR / "Scripts"
PYTHON_BIN = BIN_DIR / ("python.exe" if sys.platform.startswith("win") else "python")
PYTEST_BIN = BIN_DIR / ("pytest.exe" if sys.platform.startswith("win") else "pytest")
PIP_BIN = BIN_DIR / ("pip.exe" if sys.platform.startswith("win") else "pip")

CODE = "submitthem"
CODE_AND_DOCS = [CODE, "docs", "integration"]


def run(*args, **kwargs):
    """Run a command."""
    print(f"Running: {' '.join(str(a) for a in args)}")
    return subprocess.run(args, **kwargs)


def run_python(*args):
    """Run Python with appropriate executable."""
    python = PYTHON_BIN if USE_VENV else "python"
    return run(python, *args)


def task_which():
    """Show which Python and version."""
    run_python("--version")


def task_test():
    """Run pytest on submitthem in parallel."""
    pytest = PYTEST_BIN if USE_VENV else "pytest"
    return run(pytest, "-n", "auto", CODE)


def task_test_coverage():
    """Run pytest with coverage reporting in parallel."""
    pytest = PYTEST_BIN if USE_VENV else "pytest"
    return run(
        pytest,
        "-n",
        "auto",
        "-v",
        f"--cov={CODE}",
        "--cov-report=html",
        "--cov-report=term",
        "--durations=10",
        "--junitxml=test_results/pytest/results.xml",
        CODE,
    )


def task_format():
    """Format code with isort and black."""
    run_python("-m", "pre_commit")
    run_python("-m", "isort", *CODE_AND_DOCS)
    run_python("-m", "black", *CODE_AND_DOCS)


def task_check_format():
    """Check code formatting without modifying files."""
    run_python("-m", "isort", "--check", "--diff", *CODE_AND_DOCS)
    run_python("-m", "black", "--check", "--diff", *CODE_AND_DOCS)


def task_mypy():
    """Run mypy type checking."""
    run_python("-m", "mypy", "--version")
    return run_python("-m", "mypy", "--junit-xml=test_results/pytest/results.xml", CODE)


def task_pylint():
    """Run pylint linting."""
    run_python("-m", "pylint", "--version")
    return run_python("-m", "pylint", CODE)


def task_lint():
    """Run mypy and pylint."""
    task_mypy()
    task_pylint()


def task_venv():
    """Create virtual environment and install dependencies."""
    if VENV_DIR.exists():
        print(f"Virtual environment already exists at {VENV_DIR}")
        return

    print(f"Creating virtual environment at {VENV_DIR}...")
    run("python", "-m", "venv", str(VENV_DIR))

    print("Upgrading pip...")
    run(PIP_BIN, "install", "--progress-bar", "off", "--upgrade", "pip")

    print("Installing dependencies...")
    run(PIP_BIN, "install", "--progress-bar", "off", "-U", "-e", ".[dev]")


def task_clean():
    """Remove virtual environment."""
    if VENV_DIR.exists():
        print(f"Removing {VENV_DIR}...")
        shutil.rmtree(VENV_DIR)
    else:
        print(f"{VENV_DIR} does not exist")


def task_clean_cache():
    """Invalidate venv cache to trigger re-installation."""
    venv_pyproject = VENV_DIR / "pyproject.toml"
    if venv_pyproject.exists():
        print(f"Removing {venv_pyproject}...")
        venv_pyproject.unlink()
    else:
        print(f"{venv_pyproject} does not exist")


def task_installable_local():
    """Test that the package can be imported after installation."""
    task_venv()
    print("Testing import in venv...")
    run_python("-c", "import submitthem")


def task_installable_wheel():
    """Build and test wheel installation."""
    # Clean previous builds
    dist_dir = Path("dist")
    if dist_dir.exists():
        shutil.rmtree(dist_dir)

    # Extract version
    init_file = Path(CODE) / "__init__.py"
    version_line = next(line for line in init_file.read_text().split('\n') if '__version__' in line)
    current_version = version_line.split('"')[1]

    print(f"Current version: {current_version}")

    # Build wheel
    run_python("-m", "flit", "build", "--setup-py")

    # Create test venv
    test_venv = Path("/tmp/submitthem_user_venv") if not sys.platform.startswith("win") else Path("submitthem_user_venv_test")
    if test_venv.exists():
        shutil.rmtree(test_venv)

    run("python", "-m", "venv", str(test_venv))
    test_pip = test_venv / "bin" / "pip" if not sys.platform.startswith("win") else test_venv / "Scripts" / "pip.exe"
    test_python = test_venv / "bin" / "python" if not sys.platform.startswith("win") else test_venv / "Scripts" / "python.exe"

    # Install wheel
    wheel_file = list(dist_dir.glob("submitthem-*any.whl"))[0]
    run(test_pip, "install", "--progress-bar", "off", str(wheel_file))

    # Test import
    run(test_python, "-c", "import submitthem")


def task_installable():
    """Test both local and wheel installation."""
    task_installable_local()
    task_installable_wheel()


def task_pre_commit():
    """Run formatting and linting."""
    task_format()
    task_lint()


def task_register_pre_commit():
    """Register pre-commit hook."""
    task_venv()
    git_hook = Path(".git/hooks/pre-commit")
    if git_hook.exists():
        content = git_hook.read_text()
        if "python tasks.py pre_commit" not in content:
            git_hook.write_text(content + "\npython tasks.py pre_commit\n")
    else:
        git_hook.parent.mkdir(parents=True, exist_ok=True)
        git_hook.write_text("#!/bin/bash\npython tasks.py pre_commit\n")

    git_hook.chmod(0o755)
    print("Pre-commit hook registered")


def task_integration():
    """Run full integration tests (CI-like pipeline)."""
    print("Running integration tests...")
    task_clean_cache()
    task_venv()
    task_check_format()
    task_lint()
    task_installable()
    task_test_coverage()
    print("Integration tests completed!")


def task_all():
    """Default task: run integration."""
    task_integration()


def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        task = "all"
    else:
        task = sys.argv[1]

    # Convert task name to function name
    func_name = f"task_{task}"

    if func_name not in globals():
        print(f"Unknown task: {task}")
        print(f"Available tasks: {', '.join(name[5:] for name in globals() if name.startswith('task_'))}")
        sys.exit(1)

    try:
        globals()[func_name]()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
