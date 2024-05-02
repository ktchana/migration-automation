import os

def find_pom_files(directory):
  """
  Finds all pom.xml files within a given directory and its subdirectories.

  Args:
    directory: The path to the directory to search.

  Returns:
    A list of paths to the pom.xml files found.
  """
  pom_files = []
  for root, _, files in os.walk(directory):
    for file in files:
      if file == "pom.xml":
        pom_files.append(os.path.join(root, file))
  return pom_files

# Example usage:
directory_to_search = "/path/to/your/project"  # Replace with the actual directory path
pom_file_paths = find_pom_files(directory_to_search)

for pom_file in pom_file_paths:
  print(pom_file)