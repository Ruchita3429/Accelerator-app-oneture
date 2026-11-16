from connectors.sap.authentication import get_access_token
from connectors.sap.metadata import (
    get_sap_metadata,
    get_sap_entity_sets
)


def test_sap_connection(sap_config):
    """
    Test SAP connectivity using OAuth authentication
    and OData metadata retrieval.
    """

    try:
        # Step 1: Authenticate with SAP
        access_token = get_access_token(sap_config)

        # Step 2: Retrieve OData metadata
        metadata = get_sap_metadata(
            sap_config,
            access_token
        )

        # Step 3: Discover available entities
        entities = get_sap_entity_sets(metadata)

        return {
            "success": True,
            "message": "SAP connection successful.",
            "access_token": access_token,
            "metadata": metadata,
            "entities": entities
        }

    except Exception as error:

        return {
            "success": False,
            "message": str(error)
        }