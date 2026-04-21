# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A client and common configurations for the Search Ads 360 API."""

from google.ads.searchads360.v0.services.services.customer_service import client as customer_service_client
from google.ads.searchads360.v0.services.services.search_ads360_service import client as search_ads360_service_client
from google.oauth2.credentials import Credentials as InstalledAppCredentials
import grpc.experimental

from .config import load_from_yaml_file
from .interceptors import MetadataInterceptor

# See options at grpc.github.io/grpc/core/group__grpc__arg__keys.html
_GRPC_CHANNEL_OPTIONS = [
    ("grpc.max_metadata_size", 16 * 1024 * 1024),
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
]

DEFAULT_LOGIN_CUSTOMER_ID = "0"


class SearchAds360Client:
  """Search Ads 360 client used to configure settings and fetch services."""

  def __init__(self, credentials, refresh_token, login_customer_id):
    """Initializer for the SearchAds360Client.

    Args:
      credentials: a google.oauth2.credentials.Credentials instance.
      refresh_token: a str refresh token.
      login_customer_id: a str specifying a login customer ID.
    """

    self.credentials = credentials
    self.refresh_token = refresh_token
    self.login_customer_id = login_customer_id

  def set_ids(self, customer_id, login_customer_id):
    """Overrides login_customer_id field with value specified in parameter.

    Determine login_customer_id from multiple income sources. customer_id will
    be used as login_customer_id if login_customer_id is null.

    Args:
      customer_id: a str specifying a login customer ID.
      login_customer_id: a str specifying a login customer ID, can be null.
    """
    if login_customer_id:
      self.login_customer_id = login_customer_id
    else:
      if self.login_customer_id == "None":
        self.login_customer_id = customer_id

  @classmethod
  def load_from_file(cls, yaml_str=None):
    """Creates a SearchAds360Client with data stored in the YAML string.

    Args:
      yaml_str: a str containing YAML configuration data used to initialize a
        SearchAds360Client.

    Returns:
      A SearchAds360Client initialized with the values specified in the
      string.
    Raises:
      ValueError: If the configuration lacks a required field.
    """
    config_data = load_from_yaml_file(yaml_str)
    kwargs = cls._get_client_kwargs(config_data)
    return cls(**dict(**kwargs))

  @classmethod
  def _get_client_kwargs(cls, config_data):
    """Converts configuration dict into kwargs required by the client.

    Args:
      config_data: a dict containing client configuration.

    Returns:
      A dict containing kwargs that will be provided to the
      SearchAds360Client initializer.

    Raises:
      ValueError: If the configuration lacks a required field.
    """

    client_id = config_data.get("client_id")
    client_secret = config_data.get("client_secret")
    refresh_token = config_data.get("refresh_token")
    login_customer_id = str(config_data.get("login_customer_id"))

    return {
        "credentials":
            cls.get_credentials(cls, client_id, client_secret, refresh_token),
        "refresh_token":
            refresh_token,
        "login_customer_id":
            login_customer_id,
    }

  def get_credentials(self, client_id, client_secret, refresh_token):
    return InstalledAppCredentials(
        None,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        token_uri="https://accounts.google.com/o/oauth2/token",
    )

  def get_service(self):
    """Returns a SearchAds360 service client instance.

    Returns:
      A service client instance.
    """

    service_transport_class = search_ads360_service_client.SearchAds360ServiceClient.get_transport_class(  # pylint: disable=line-too-long
    )

    endpoint = (
        search_ads360_service_client.SearchAds360ServiceClient.DEFAULT_ENDPOINT
        )
    channel = service_transport_class.create_channel(
        host=endpoint,
        credentials=self.credentials,
        options=_GRPC_CHANNEL_OPTIONS,
    )

    interceptors = [
        MetadataInterceptor(
            self.login_customer_id,
        ),
    ]

    channel = grpc.intercept_channel(channel, *interceptors)

    service_transport = service_transport_class(channel=channel)

    return search_ads360_service_client.SearchAds360ServiceClient(
        transport=service_transport
    )

  def get_customer_service(self):
    """Returns a customer service client instance.

    Returns:
      A customer service client instance.
    """

    service_transport_class = (
        customer_service_client.CustomerServiceClient.get_transport_class()
    )

    endpoint = customer_service_client.CustomerServiceClient.DEFAULT_ENDPOINT
    channel = service_transport_class.create_channel(
        host=endpoint,
        credentials=self.credentials,
        options=[],
    )

    if self.login_customer_id == "None":
      self.login_customer_id = DEFAULT_LOGIN_CUSTOMER_ID

    interceptors = [
        MetadataInterceptor(
            self.login_customer_id,
        ),
    ]

    channel = grpc.intercept_channel(channel, *interceptors)

    service_transport = service_transport_class(channel=channel)

    return customer_service_client.CustomerServiceClient(
        transport=service_transport
    )
