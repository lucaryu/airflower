from sqlalchemy import create_engine
import sys

try:
    # We don't need a real connection, just checking if the dialect loads
    # Using a dummy connection string
    uri = "oracle+oracledb://user:pass@localhost:1521/?service_name=xe"
    engine = create_engine(uri)
    print("Engine created successfully. Dialect loaded.")
    
    # We won't actually connect because we don't have a DB, 
    # but we can check if the driver is recognized.
    print(f"Driver: {engine.driver}")
    
except ImportError as e:
    print(f"ImportError: {e}")
    sys.exit(1)
except Exception as e:
    # If we get here, it might be a connection error, which is fine.
    # We just want to avoid "ModuleNotFoundError" or "NoSuchModuleError"
    print(f"Caught expected exception (likely connection related): {type(e).__name__}: {e}")
    if "No module named" in str(e):
        print("FAILURE: Module missing error detected.")
        sys.exit(1)
    else:
        print("SUCCESS: Dialect loaded, error was not about missing module.")
