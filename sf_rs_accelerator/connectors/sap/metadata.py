import urllib.request
import urllib.error
import xml.etree.ElementTree as ET


def get_sap_metadata(sap_config, access_token):
    """
    Fetch SAP OData service metadata.
    """

    metadata_url = (
        f"{sap_config['odata_base_url']}/$metadata"
    )

    request = urllib.request.Request(
        metadata_url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/xml"
        }
    )

    try:
        with urllib.request.urlopen(
            request, timeout=30
        ) as response:

            metadata_xml = response.read()

        return ET.fromstring(metadata_xml)

    except urllib.error.HTTPError as error:
        raise Exception(
            f"Failed to retrieve SAP metadata: HTTP {error.code}"
        )

    except urllib.error.URLError as error:
        raise Exception(
            f"SAP metadata connection failed: {error}"
        )


def get_sap_entity_sets(metadata_root):
    """
    Discover available entity sets from SAP metadata.
    """

    entity_sets = []

    for entity_set in metadata_root.iter():
        if entity_set.tag.endswith("EntitySet"):

            entity_sets.append({
                "name": entity_set.attrib.get("Name"),
                "entity_type": entity_set.attrib.get("EntityType")
            })

    return entity_sets


def get_sap_entity_columns(metadata_root, entity_name):
    """
    Retrieve column information for a selected entity.
    """

    entity_type = None

    # Find the EntityType associated with the selected entity
    for entity_set in metadata_root.iter():

        if (
            entity_set.tag.endswith("EntitySet")
            and entity_set.attrib.get("Name") == entity_name
        ):
            entity_type = entity_set.attrib.get("EntityType")
            break

    if not entity_type:
        raise ValueError(
            f"Entity '{entity_name}' not found."
        )

    # Find the corresponding EntityType definition
    for element in metadata_root.iter():

        if (
            element.tag.endswith("EntityType")
            and element.attrib.get("Name") == entity_type.split(".")[-1]
        ):

            columns = []

            for prop in element:

                if prop.tag.endswith("Property"):

                    columns.append({
                        "name": prop.attrib.get("Name"),
                        "type": prop.attrib.get("Type"),
                        "nullable": prop.attrib.get("Nullable", "true")
                    })

            return columns

    return []