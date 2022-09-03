#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .streams import Workspace


class SourceElevateRockset(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:  #we will be testing if the input is valid and working correctly. we have two inputs, one is the name of the lambda key or the workspace and the other input is the API key
        try:
            workspace_stream = Workspace(
                authenticator=TokenAuthenticator(token=config["api_token"], auth_header="Authorization", auth_method="ApiKey"),
                workspace=config["workspace"],
            )
            next(workspace_stream.read_records(
                sync_mode=SyncMode.full_refresh))   
            return True, None
        except Exception as e:
            return False, f"Please check that your API key and workspace name are entered correctly: {repr(e)}"

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:

        stream_kwargs = {
            "authenticator": TokenAuthenticator(config["api_token"]),
            "workspace": config["workspace"],
        }

        return [Workspace(**stream_kwargs)]
