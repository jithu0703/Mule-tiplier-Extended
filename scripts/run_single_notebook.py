"""Utility to execute Jupyter notebooks cleanly with nbclient, saving outputs and logging errors."""

import sys
import os
import asyncio

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import nbformat
from nbclient import NotebookClient
from nbclient.exceptions import CellExecutionError

def run_notebook(notebook_path: str, timeout: int = 600) -> bool:
    print(f"\n=======================================================")
    print(f"Executing notebook: {notebook_path}")
    print(f"=======================================================")
    
    nb_abs = os.path.abspath(notebook_path)
    nb_dir = os.path.dirname(nb_abs)
    
    with open(nb_abs, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)
        
    client = NotebookClient(
        nb,
        timeout=timeout,
        kernel_name="chain-reaction-env",
        resources={"metadata": {"path": nb_dir}},
    )
    
    try:
        client.execute()
        with open(nb_abs, "w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        print(f"SUCCESS: {notebook_path} executed and saved successfully.")
        return True
    except CellExecutionError as exc:
        print(f"\nFAILED in {notebook_path}:")
        print(str(exc))
        # Save whatever outputs were collected up to failure point
        with open(nb_abs, "w", encoding="utf-8") as f:
            nbformat.write(nb, f)
        return False
    except Exception as exc:
        print(f"\nUNEXPECTED ERROR in {notebook_path}: {exc}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run_single_notebook.py <path_to_notebook>")
        sys.exit(1)
    success = run_notebook(sys.argv[1])
    sys.exit(0 if success else 1)
