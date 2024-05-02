import xml.etree.ElementTree as ET

def get_dependencies(pom_file):
  """
  Parses a pom.xml file, extracts dependencies, and resolves version variables from properties.

  Args:
    pom_file: The path to the pom.xml file.

  Returns:
    A list of tuples, where each tuple contains (groupId, artifactId, version) of a dependency.
  """
  tree = ET.parse(pom_file)
  root = tree.getroot()

  # Get properties
  properties = {prop.find('name').text: prop.find('value').text for prop in root.find('properties')}

  dependencies = []
  for dependency in root.iter('dependency'):
    groupId = dependency.find('groupId').text
    artifactId = dependency.find('artifactId').text
    version_element = dependency.find('version')
    version = version_element.text

    # Resolve version if it's a property
    if version.startswith("${") and version.endswith("}"):
      property_name = version[2:-1]
      version = properties.get(property_name, version)  # Use version as-is if property not found

    dependencies.append((groupId, artifactId, version))

  return dependencies

# Example usage (same as before)