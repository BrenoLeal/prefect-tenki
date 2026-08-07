from prefect_tenki.credentials import TenkiCredentials


def test_credentials_mask_api_key_and_build_explicit_client_options():
    credentials = TenkiCredentials(
        api_key="tk_test_secret",
        api_endpoint=" https://api.example.test/ ",
    )

    assert str(credentials.api_key) == "**********"
    assert "tk_test_secret" not in repr(credentials)
    assert credentials.get_client_options() == {
        "auth_token": "tk_test_secret",
        "base_url": "https://api.example.test",
    }


def test_empty_api_endpoint_uses_sdk_default():
    credentials = TenkiCredentials(api_key="tk_test_secret", api_endpoint="  ")

    assert credentials.api_endpoint is None
    assert credentials.get_client_options() == {"auth_token": "tk_test_secret"}
