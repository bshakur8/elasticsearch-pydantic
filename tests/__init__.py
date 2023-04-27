from pydantic import validator

from es_pydantic import field
from es_pydantic import validators
from es_pydantic.model import ESModel

DEFAULT_DATETIME_FORMAT = r"strict_date_optional_time_nanos||epoch_millis"


class Shirt(ESModel):
    brand: field.Keyword()
    color: field.Keyword()

    class Meta:
        index = "shirts"
        enable_source = True
        version = 1

    class Settings:
        number_of_shards = 1
        number_of_replicas = 1

    @validator("color")
    @classmethod
    def validate_color(cls, color):
        if color not in ["black", "red"]:
            raise ValueError("Invalid color")
        return color


class EventLog(ESModel):
    timestamp: field.DateTime(format=DEFAULT_DATETIME_FORMAT)
    guid: field.Text()
    object_type: field.Text()

    class Meta:
        index = "event_log"
        enable_source = True

    class Settings:
        number_of_shards = 1
        number_of_replicas = 1

    # validators
    v1 = validators.guid("guid", always=True)
