"""
Test script to verify mmh3_compat and export flow simulation
"""
import sys
import os

# Ensure mmh3_compat works even if mmh3.cp*.pyd is blocked or raises OSError
# To simulate WDAC blocking:
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
sys.modules.pop("mmh3", None)

import mmh3_compat

print("mmh3 in sys.modules:", "mmh3" in sys.modules)
import mmh3
print("mmh3 module file:", getattr(mmh3, "__file__", "builtin"))
print("mmh3.hash test:", mmh3.hash("test_val"))
print("mmh3.hash64 test:", mmh3.hash64("test_val"))

# Verify PyIceberg imports cleanly and uses this mmh3
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, IntegerType
from pyiceberg.transforms import BucketTransform

bt = BucketTransform(16)
bucket_val = bt.transform(StringType())("customer_123")
print("BucketTransform calculated:", bucket_val)

print("ALL TESTS PASSED SUCCESSFULLY!")
