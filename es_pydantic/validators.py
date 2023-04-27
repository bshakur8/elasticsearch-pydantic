import uuid
from typing import Union

from pydantic import validator
from pyinline import inline


@inline
def guid(*fields, **kwargs) -> validator:
    def validate_guid(data: Union[str, uuid.UUID]) -> str:
        try:
            guid_str = str(data)
            if isinstance(data, uuid.UUID):
                return guid_str
            uuid.UUID(guid_str)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid GUID field {data}: {str(e)}") from None
        return guid_str

    allow_reuse = kwargs.pop("allow_reuse", True)
    return validator(*fields, allow_reuse=allow_reuse, **kwargs)(validate_guid)
