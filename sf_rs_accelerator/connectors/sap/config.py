def create_sap_config(
    token_url,
    client_id,
    client_secret,
    odata_base_url,
    entity_set,
    page_size=1000
):
    """
    Create and validate SAP connection configuration.
    """

    config = {
        "token_url": token_url.strip(),
        "client_id": client_id.strip(),
        "client_secret": client_secret,
        "odata_base_url": odata_base_url.strip().rstrip("/"),
        "entity_set": entity_set.strip(),
        "page_size": int(page_size)
    }

    # Validate required fields
    for key in [
        "token_url",
        "client_id",
        "client_secret",
        "odata_base_url",
        "entity_set"
    ]:
        if not config[key]:
            raise ValueError(f"{key} is required.")

    if config["page_size"] <= 0:
        raise ValueError("Page size must be greater than zero.")

    return config