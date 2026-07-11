import sys
tools = {
    "Python": (sys.version_info >= (3, 10), sys.version),
    "PySpark": None,
    "Pandas": None,
    "PyArrow": None,
    "dbt-core": None,
    "Great Expectations": None,
}

# Check each library
try:
    import pyspark
    tools["PySpark"] = (True, pyspark.__version__)
except ImportError:
    tools["PySpark"] = (False, "NOT INSTALLED")
try:
    import pandas
    tools["Pandas"] = (True, pandas.__version__)
except ImportError:
    tools["Pandas"] = (False, "NOT INSTALLED")
try:
    import pyarrow
    tools["PyArrow"] = (True, pyarrow.__version__)
except ImportError:
    tools["PyArrow"] = (False, "NOT INSTALLED")
try:
    # Use the canonical version API — dbt.__version__ is not reliably set
    # across all dbt-core versions and raises AttributeError on some installs.
    from dbt.version import get_installed_version
    tools["dbt-core"] = (True, str(get_installed_version()).lstrip("="))
except ImportError:
    tools["dbt-core"] = (False, "NOT INSTALLED")
try:
    import great_expectations
    tools["Great Expectations"] = (True, great_expectations.__version__)
except ImportError:
    tools["Great Expectations"] = (False, "NOT INSTALLED")
print("\n=== Big Data Engineering Environment Verification ===\n")
all_ok = True
for tool, result in tools.items():
    if result is None:
        continue
    status = "OK " if result[0] else "MISSING"
    print(f" [{status}] {tool}: {result[1]}")
    if not result[0]:
        all_ok = False
print()
if all_ok:
    print("All tools verified. Your environment is ready for the course.")
else:
    print("Some tools are missing. Please re-run the setup steps above.")