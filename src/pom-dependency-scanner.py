import xml.etree.ElementTree as ET
import re
import json

def get_properties(parent_node, xmlns):
  """Extracts properties from the pom.xml file as a dictionary."""
  return {prop.tag.replace(xmlns, ""): prop.text for prop in parent_node.find(xmlns + 'properties')}

def replace_vars(str_to_replace, properties):
  """Replaces variables in a string with their corresponding values from the properties dictionary."""
  propertyRe = re.compile("\$\{([^\}]+)\}")
  for var in propertyRe.findall(str_to_replace):
    if var in properties:
      str_to_replace = str_to_replace.replace("${" + var + "}", properties[var])
  return str_to_replace

def get_dependencies(parent_node, xmlns, properties):
  """Retrieves dependencies from a specified section in the pom.xml file."""
  return get_dependent_versions("dependencies", parent_node, xmlns, properties)

def get_plugins(parent_node, xmlns, properties):
  """Retrieves plugins from a specified section in the pom.xml file."""
  return get_dependent_versions("plugins", parent_node, xmlns, properties)

def get_dependent_versions(dependency_type, parent_node, xmlns, properties):
  """Extracts dependencies or plugins with their groupId, artifactId, and version."""
  dependencies = []
  for dependenciesSection in parent_node.iter(xmlns + dependency_type):
    for dependency in dependenciesSection:
      groupId = dependency.find(xmlns + 'groupId').text
      artifactId = dependency.find(xmlns + 'artifactId').text
      version_element = dependency.find(xmlns + 'version')
      version = "(null)"
      if version_element is not None:
        version = version_element.text
      
      # Resolve version if it's a property
      artifactId = replace_vars(artifactId, properties)
      version = replace_vars(version, properties)
      
      dependencies.append((groupId, artifactId, version))
  
  return dependencies

def print_dependencies(dependencies):
  """Prints the dependencies in a sorted format."""
  # Sort by ArtifactId:
  sorted_dependencies = sorted(dependencies, key=lambda d: d[1])

  for groupId, artifactId, version in sorted_dependencies:
    print(f"ArtifactId: {artifactId}, GroupId: {groupId}, Version: {version}")

def main():
  """Parses the pom.xml file and extracts dependencies and plugins."""
  pom_file_path = "pom.xml"  # Replace with the actual pom.xml file path

  tree = ET.parse(pom_file_path)
  root = tree.getroot()

  #xmlns = '{http://maven.apache.org/POM/4.0.0}'
  xmlns = re.compile("project$").sub("", root.tag)

  # Get pom.xml variables from <properties>
  properties = get_properties(root, xmlns)

  # List all dependencies in the root <dependencies> section
  dependencies = get_dependencies(root, xmlns, properties)
  print("### all direct dependencies ###")
  print_dependencies(dependencies)

  # List all dependencies in the root <dependenciesManagement> section
  dependency_management_node = root.find(xmlns + 'dependencyManagement')
  dependencies = get_dependencies(dependency_management_node, xmlns, properties)
  print("### all central dependencies in dependencyManagement ###")
  print_dependencies(dependencies)

  # List all <plugins> directly under the <build> section
  plugin_management_node = root.find(xmlns + 'build')
  dependencies = get_plugins(plugin_management_node, xmlns, properties)
  print("### all direct build dependencies ###")
  print_dependencies(dependencies)

  # List all <plugins> under the <build> -> <pluginManagement> section
  plugin_management_node = root.find(xmlns + 'build').find(xmlns + 'pluginManagement')
  dependencies = get_plugins(plugin_management_node, xmlns, properties)
  print("### all central build dependencies in pluginManagement ###")
  print_dependencies(dependencies)

main()