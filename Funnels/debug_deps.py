
import sys
import pkg_resources

print(f"Python: {sys.executable}")
print(f"Path: {sys.path}")

try:
    import playwright
    print("✓ Imported playwright")
except ImportError as e:
    print(f"✗ Failed to import playwright: {e}")

try:
    from playwright.sync_api import sync_playwright
    print("✓ Imported sync_playwright")
except ImportError as e:
    print(f"✗ Failed to import sync_playwright: {e}")

try:
    import playwright_stealth
    print("✓ Imported playwright_stealth")
except ImportError as e:
    print(f"✗ Failed to import playwright_stealth: {e}")

# Check installed packages
packages = [d for d in pkg_resources.working_set]
for p in packages:
    if "playwright" in p.project_name.lower():
        print(f"Installed: {p.project_name} {p.version}")
