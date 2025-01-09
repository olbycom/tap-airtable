import base64
import json
import logging
import urllib.parse
from copy import deepcopy

import singer
from custom_logger import internal_logger, user_logger
from requests import Session
from requests.adapters import HTTPAdapter, Retry
from singer import metadata
from singer.catalog import Catalog, CatalogEntry, Schema


def write_secrets(config: dict) -> None:
    import json
    import sys

    if "token" not in config or "refresh_token" not in config:
        return

    secrets = {
        "type": "CREDENTIALS_CHANGED",
        "secret": {"access_token": config["token"], "refresh_token": config["refresh_token"], "token_type": "Bearer"},
    }
    message = json.dumps(secrets)
    sys.stdout.write(message + "\n")
    sys.stdout.flush()


def init_session() -> Session:
    session = Session()

    retries = Retry(total=10, backoff_factor=2, status_forcelist=[500, 502, 503, 504, 429])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    return session


class CatalogEntry(CatalogEntry):

    def to_dict(self):
        result = super(CatalogEntry, self).to_dict()
        if self.group:
            result["group"] = self.group
        return result


class Airtable(object):
    metadata_url = "https://api.airtable.com/v0/meta/bases/"
    records_url = "https://api.airtable.com/v0/"
    token_url = "https://airtable.com/oauth2/v1/token"
    token = None
    selected_by_default = False
    remove_emojis = False
    session = init_session()

    @classmethod
    def refresh_token(cls, config):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": "Basic "
            + base64.b64encode(f"{config['client_id']}:{config['client_secret']}".encode("utf-8")).decode("utf-8"),
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": config["refresh_token"],
            "scope": "data.records:read data.recordComments:read schema.bases:read user.email:read",
        }

        response = cls.session.post(cls.token_url, headers=headers, data=data)

        if response.status_code == 200:
            refreshed_creds = response.json()

            config["token"] = refreshed_creds["access_token"]
            cls.token = refreshed_creds["access_token"]
            config["refresh_token"] = refreshed_creds["refresh_token"]
            config["refreshed"] = True

            cls.__apply_config(config)
            write_secrets(config)

            internal_logger.info("Token refreshed successfully")
        else:
            internal_logger.warning(f"Failed to refresh token: {response.status_code} -> {response.json()}")

            response_dict = response.json()

            if response_dict.get("error_description", "") == "Invalid token.":
                user_logger.error("Refresh token has expired. Please update your configuration with a new token.")
                raise Exception("Refresh token has expired. Please re-authenticate to make it work again.")

        return config

    @classmethod
    def run_discovery(cls, args):
        cls.__apply_config(args.config)
        if "base_id" in args.config:
            base_id = args.config["base_id"]
            entries = cls.discover_base(base_id)
            return Catalog(entries).dump()

        bases = cls.__get_base_ids()
        entries = []

        for base in bases:
            entries.extend(cls.discover_base(base["id"], base["name"]))
            if args.config.get("validate_only", False):
                break

        return Catalog(entries).dump()

    @classmethod
    def __apply_config(cls, config):
        if "metadata_url" in config:
            cls.metadata_url = config["metadata_url"]
        if "records_url" in config:
            cls.records_url = config["records_url"]
        if "selected_by_default" in config:
            cls.selected_by_default = config["selected_by_default"]
        if "remove_emojis" in config:
            cls.remove_emojis = config["remove_emojis"]
        cls.token = config["token"]

    @classmethod
    def __get_base_ids(cls):
        response = cls.session.get(cls.metadata_url, headers=cls.__get_auth_header())
        response.raise_for_status()
        bases = []
        for base in response.json()["bases"]:
            bases.append({"id": base["id"], "name": base["name"]})
        return bases

    @classmethod
    def __get_auth_header(cls):
        return {"Authorization": "Bearer {}".format(cls.token)}

    @classmethod
    def discover_base(cls, base_id, base_name=None):
        internal_logger.info("discover base " + base_id)
        headers = cls.__get_auth_header()
        response = cls.session.get(url=cls.metadata_url + base_id + "/tables", headers=headers)
        response.raise_for_status()
        entries = []

        for table in response.json()["tables"]:
            schema_cols = {"_airtable_record_id": Schema(inclusion="automatic", type=["null", "string"])}

            meta = {}

            table_name = table["name"]
            keys = []
            meta = metadata.write(meta, (), "inclusion", "available")
            meta = metadata.write(meta, "database_name", "base_id", base_id)

            for field in table["fields"]:
                # numbers are not allowed at the start of column name in big query
                # check if the name starts with digit, keep the same naming but add a character before
                field_name = field["name"]

                if field["name"][0].isdigit():
                    field_name = "c_" + field_name

                col_schema = cls.column_schema(field)
                if col_schema.inclusion == "automatic":
                    keys.append(field_name)

                if field_name in schema_cols:
                    field_ids = metadata.get(meta, ("properties", field_name), "airtable_field_ids") or []
                    field_ids.append(field["id"])

                    meta = metadata.write(
                        meta,
                        ("properties", field_name),
                        "airtable_field_ids",
                        field_ids,
                    )
                    continue

                schema_cols[field_name] = col_schema

                meta = metadata.write(meta, ("properties", field_name), "inclusion", "available")
                meta = metadata.write(meta, ("properties", field_name), "real_name", field["name"])
                meta = metadata.write(
                    meta,
                    ("properties", field_name),
                    "airtable_type",
                    field["type"] or None,
                )
                meta = metadata.write(
                    meta,
                    ("properties", field_name),
                    "airtable_field_ids",
                    [field["id"]],
                )

            schema = Schema(type="object", properties=schema_cols)
            entry = CatalogEntry(
                tap_stream_id=table["id"],
                database=base_name or base_id,
                table=table_name,
                stream=table["id"],
                metadata=metadata.to_list(meta),
                key_properties=keys,
                schema=schema,
            )

            # Differ tables from base name
            setattr(entry, "group", base_name)
            entries.append(entry)

        return entries

    @classmethod
    def column_schema(cls, col_info):
        date_types = ["dateTime"]
        number_types = ["number", "autoNumber"]
        pk_types = ["autoNumber"]

        air_type = "string"

        if "config" in col_info and "type" in col_info["config"]:
            air_type = col_info["config"]["type"]

        inclusion = "available"
        if air_type in pk_types:
            inclusion = "automatic"

        schema = Schema(inclusion=inclusion)

        singer_type = "string"
        if air_type in number_types:
            singer_type = "number"

        schema.type = ["null", singer_type]

        if air_type in date_types:
            schema.format = "date-time"
        if air_type in ["date"]:
            schema.format = "date"

        return schema

    @classmethod
    def _find_base_id(cls, schema):
        for m in schema["metadata"]:
            if "breadcrumb" in m and m["breadcrumb"] == "database_name":
                return m["metadata"]["base_id"]

        raise Exception("catalog schema is missing base id")

    @classmethod
    def _find_selected_columns(cls, schema):
        selected_cols = {}
        field_ids = []
        for m in schema["metadata"]:
            if "properties" not in m["breadcrumb"]:
                continue

            if "selected" in m["metadata"] and m["metadata"]["selected"]:
                column_name = m["breadcrumb"][1]
                ids = m["metadata"].get("airtable_field_ids", [])
                selected_cols[column_name] = schema["schema"]["properties"][column_name]
                field_ids.extend(ids)
        return selected_cols, field_ids

    @classmethod
    def _find_column(cls, col, meta_data):
        for m in meta_data:
            if "breadcrumb" in m and "properties" in m["breadcrumb"] and m["breadcrumb"][1] == col:
                if m["metadata"].get("real_name"):
                    return m["metadata"]["real_name"]

    @classmethod
    def run_sync(cls, config, properties):
        cls.__apply_config(config)

        streams = properties["streams"]

        for stream in streams:
            schema = stream["schema"]["properties"]
            base_id = cls._find_base_id(stream)
            table = stream["table_name"]
            col_defs, field_ids = cls._find_selected_columns(stream)

            counter = 0
            if len(col_defs) > 0:
                internal_logger.info("will import " + table)

                response = Airtable.get_response(base_id, table, field_ids, counter=counter)
                records = response.json().get("records")

                if records:
                    col_schema = deepcopy(col_defs)
                    col_schema["_airtable_record_id"] = schema["_airtable_record_id"]

                    # replace % and # to avoid duplicated columns
                    col_schema = {k.replace("%", "percent").replace("#", "number"): v for k, v in col_schema.items()}

                    singer.write_schema(stream["tap_stream_id"], {"properties": col_schema}, stream["key_properties"])
                    singer.write_records(stream["tap_stream_id"], cls._map_records(stream, records))
                    offset = response.json().get("offset")

                    while offset:
                        counter += 1
                        response = Airtable.get_response(base_id, table, field_ids, offset, counter=counter)
                        records = response.json().get("records")
                        if records:
                            singer.write_records(stream["tap_stream_id"], cls._map_records(stream, records))
                            offset = response.json().get("offset")

    @classmethod
    def _map_records(cls, stream, records):
        mapped = []
        schema = stream["schema"]["properties"]
        meta_data = stream["metadata"]
        for r in records:
            row = {}
            for col in schema:
                col_def = schema[col]
                requested_type = col_def["type"][1] or "string"

                col_name = cls._find_column(col, meta_data) or col
                val = r["fields"].get(col_name)
                if val is not None:
                    val = cls.cast_type(val, requested_type)
                row[col] = val

            # rename original columnd ID to avoid duplicate conflicts with user created columns with the same name
            row["_airtable_record_id"] = r["id"]

            # replace % and # to avoid duplicated columns
            row = {k.replace("%", "percent").replace("#", "number"): v for k, v in row.items()}

            # TODO: cast to string/numbers?
            mapped.append(row)
        return mapped

    @classmethod
    def cast_type(cls, val, requested_type):
        col_type = type(val)

        if requested_type == "string" and col_type is not str:
            if col_type is float or col_type is int:
                return str(val)
            return json.dumps(val)
        return val

    @classmethod
    def get_response(cls, base_id, table, fields, offset=None, counter=0):
        table = urllib.parse.quote(table, safe="")
        uri = cls.records_url + base_id + "/" + table

        uri += "?"
        params = {}

        if fields:
            params["fields[]"] = list(fields)
        if offset:
            params["offset"] = offset

        uri += urllib.parse.urlencode(params, True)

        response = cls.session.get(uri, headers=cls.__get_auth_header())

        internal_logger.info(
            "METRIC "
            + json.dumps(
                {
                    "type": "counter",
                    "metric": "page",
                    "value": counter,
                }
            )
        )
        if response.status_code != 200:
            internal_logger.info(
                "REASON "
                + json.dumps(
                    {
                        "value": response.text,
                    }
                )
            )
        response.raise_for_status()
        return response
