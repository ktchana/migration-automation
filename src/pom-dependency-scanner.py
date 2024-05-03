import xml.etree.ElementTree as ET
import re
import json

def replace_vars(str_to_replace, properties):
    """
    Replaces variables in a string with their corresponding values from a properties dictionary.

    Args:
        str_to_replace: The string containing variables to be replaced.
        properties: A dictionary containing variable names as keys and their values.

    Returns:
        The string with variables replaced by their values.
    """
    propertyRe = re.compile("\$\{([^\}]+)\}")
    for var in propertyRe.findall(str_to_replace):
        if var in properties:
            str_to_replace = str_to_replace.replace("${" + var + "}", properties[var])
    return str_to_replace

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

    xmlns = '{http://maven.apache.org/POM/4.0.0}'
    # Get properties
    properties = {prop.tag.replace(xmlns, ""): prop.text for prop in root.find(xmlns + 'properties')}

    dependencies = []
    for dependenciesSection in root.iter(xmlns + 'dependencies'):
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

# Example usage:
pom_file_path = "pom.xml"  # Replace with the actual pom.xml file path
dependencies = get_dependencies(pom_file_path)

# Sort by ArtifactId:
sorted_dependencies = sorted(dependencies, key=lambda d: d[1])

for groupId, artifactId, version in sorted_dependencies:
    print(f"ArtifactId: {artifactId}, GroupId: {groupId}, Version: {version}")

