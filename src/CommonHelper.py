import os

# Resolve the path to the project root directory
# This is useful for ensuring that file paths are correct regardless of where the script is run from.
# The project root is assumed to be the parent directory of the 'src' directory.
def resolve_path(relative_path):
    # Find the src directory from the current script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Determine project root (parent of src directory)
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    
    # Join with the requested relative path
    absolute_path = os.path.join(project_root, relative_path)
    
    return absolute_path