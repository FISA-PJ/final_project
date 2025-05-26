# verify_modules.py
print("=== Custom Module Verification ===")

try:
    import pypdf
    print(f"✓ pypdf version: {pypdf.__version__}")
    print(f"✓ pypdf location: {pypdf.__file__}")
except Exception as e:
    print(f"✗ pypdf check failed: {e}")

try:
    import langchain
    from langchain.retrievers.parent_document_retriever import ParentDocumentRetriever
    print(f"✓ langchain location: {langchain.__file__}")
    print("✓ ParentDocumentRetriever imported successfully")
except Exception as e:
    print(f"✗ langchain check failed: {e}")

print("=== End Verification ===")