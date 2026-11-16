import urllib.request
import urllib.error
import urllib.parse
import xml.etree.ElementTree as ET
import time


def read_odata_page(
    sap_config,
    access_token,
    offset,
    max_retries=5
):
    """
    Fetch one page of records from SAP OData V2.
    """

    # Construct entity endpoint
    entity_url = (
        f"{sap_config['odata_base_url']}/"
        f"{sap_config['entity_set']}"
    )

    # Prepare pagination parameters
    query = urllib.parse.urlencode({
        "$top": sap_config["page_size"],
        "$skip": offset
    })

    url = f"{entity_url}?{query}"

    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/atom+xml",
            "Authorization": f"Bearer {access_token}"
        }
    )

    for attempt in range(max_retries + 1):

        try:
            with urllib.request.urlopen(
                request, timeout=120
            ) as response:

                xml_response = response.read().decode("utf-8")

            root = ET.fromstring(xml_response)

            # Extract records from Atom XML
            entries = root.findall(
                "{http://www.w3.org/2005/Atom}entry"
            )

            rows = []

            for entry in entries:

                properties = entry.find(
                    ".//{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}properties"
                )

                if properties is None:
                    continue

                record = {
                    prop.tag.split("}")[-1]: prop.text
                    for prop in properties
                }

                rows.append(record)

            return rows

        except urllib.error.HTTPError as error:

            if error.code not in (429, 500, 502, 503, 504):
                raise Exception(
                    f"OData request failed: HTTP {error.code}"
                )

            if attempt == max_retries:
                raise Exception(
                    "OData request failed after retries."
                )

        except (
            urllib.error.URLError,
            TimeoutError,
            OSError
        ) as error:

            if attempt == max_retries:
                raise Exception(
                    f"OData connection failed: {error}"
                )

        time.sleep(min(60, 2 ** (attempt + 1)))


def fetch_all_odata_records(sap_config, access_token):
    """
    Fetch all records from the SAP OData entity using pagination.
    """

    all_rows = []
    offset = 0
    page_size = sap_config["page_size"]

    while True:

        print(
            f"Fetching records: offset={offset}, "
            f"page_size={page_size}"
        )

        rows = read_odata_page(
            sap_config,
            access_token,
            offset
        )

        page_count = len(rows)

        if page_count == 0:
            break

        all_rows.extend(rows)

        print(
            f"Retrieved {page_count} records. "
            f"Total records: {len(all_rows)}"
        )

        offset += page_count

        if page_count < page_size:
            break

    print(f"Total records extracted: {len(all_rows)}")

    return all_rows