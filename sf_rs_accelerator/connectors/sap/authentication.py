import base64
import json
import time
import urllib.parse
import urllib.request
import urllib.error


def get_access_token(sap_config, max_retries=3):
    """
    Authenticate with SAP using OAuth 2.0 Client Credentials Flow.
    Returns the access token.
    """

    # Extract credentials
    token_url = sap_config["token_url"]
    client_id = sap_config["client_id"]
    client_secret = sap_config["client_secret"]

    # Encode Client ID and Client Secret
    credentials = f"{client_id}:{client_secret}"

    encoded_credentials = base64.b64encode(
        credentials.encode("utf-8")
    ).decode("ascii")

    # Prepare OAuth request payload
    payload = urllib.parse.urlencode({
        "grant_type": "client_credentials"
    }).encode("utf-8")

    # Prepare HTTP request
    request = urllib.request.Request(
        token_url,
        data=payload,
        headers={
            "Authorization": f"Basic {encoded_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        },
        method="POST"
    )

    # Execute request with retry handling
    for attempt in range(max_retries + 1):

        try:
            with urllib.request.urlopen(request, timeout=30) as response:

                token_response = json.loads(
                    response.read().decode("utf-8")
                )

            access_token = token_response["access_token"]

            return access_token

        except urllib.error.HTTPError as error:

            response_body = error.read().decode(
                "utf-8", errors="replace"
            )[:500]

            if error.code not in (429, 500, 502, 503, 504):
                raise Exception(
                    f"SAP authentication failed: "
                    f"HTTP {error.code}: {response_body}"
                )

            if attempt == max_retries:
                raise Exception(
                    f"SAP authentication failed after "
                    f"{max_retries} retries."
                )

        except (urllib.error.URLError, TimeoutError, OSError) as error:

            if attempt == max_retries:
                raise Exception(
                    f"SAP authentication network error: {error}"
                )

        delay = min(30, 2 ** (attempt + 1))
        time.sleep(delay)