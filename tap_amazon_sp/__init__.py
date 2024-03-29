#!/usr/bin/env python3
import os
import json
import singer
from singer import utils, metadata, Transformer
from sp_api.base import Marketplaces

from tap_amazon_sp.context import Context

# REQUIRED_CONFIG_KEYS = ["access_token", "aws_access_key_id", "aws_secret_access_key", "arn_role"]
REQUIRED_CONFIG_KEYS = []

LOGGER = singer.get_logger()


def set_context_streams():
    from tap_amazon_sp.streams import models
    for model in models:
        Context.stream_objects[model.name] = model


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    schemas = {}

    schema_path = get_abs_path('schemas')
    files = [f for f in os.listdir(schema_path) if os.path.isfile(os.path.join(schema_path, f))]
    for filename in files:
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = json.load(file)
    return schemas


def load_schema_references():
    shared_schema_file = "definitions.json"
    shared_schema_path = get_abs_path('schemas/')

    refs = {}
    with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
        refs[shared_schema_file] = json.load(data_file)

    return refs


def discover():
    raw_schemas = load_schemas()
    streams = []

    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        # create and add catalog entry

        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': singer.resolve_schema_references(schema),
            'metadata': get_discovery_metadata(stream, schema),
            'key_properties': stream.key_properties,
            'replication_key': stream.replication_key,
            'replication_method': stream.replication_method
        }

        streams.append(catalog_entry)

    return {'streams': streams}


def get_discovery_metadata(stream, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    for field_name in schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)


def sync():
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):
            singer.write_schema(stream["tap_stream_id"],
                                stream["schema"],
                                stream["key_properties"],
                                bookmark_properties=stream["replication_key"])
            Context.counts[stream["tap_stream_id"]] = 0

    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream = Context.stream_objects[stream_id]()
        if not Context.is_selected(stream_id):
            LOGGER.info('Skipping stream: %s', stream_id)
            continue

        LOGGER.info('Syncing stream: %s', stream_id)

        if not Context.state.get('bookmarks'):
            Context.state['bookmarks'] = {}

        Context.state['bookmarks']['currently_sync_stream'] = stream_id

        marketplace_code = Context.config.get("marketplace_code", "US")

        stream.set_marketplace(get_mapped_marketplace(marketplace_code))

        with Transformer() as transformer:
            for rec in stream.sync():
                rec = stream.transform_fields(rec)
                extraction_time = singer.utils.now()
                record_schema = catalog_entry['schema']
                record_metadata = metadata.to_map(catalog_entry['metadata'])
                rec = transformer.transform(rec, record_schema, record_metadata)
                singer.write_record(stream_id,
                                    rec,
                                    time_extracted=extraction_time)
                Context.counts[stream_id] += 1

            Context.state['bookmarks'].pop('currently_sync_stream')
            singer.write_state(Context.state)

    return


def set_env_variables():
    os.environ["SP_API_REFRESH_TOKEN"] = Context.config["refresh_token"]

    os.environ["LWA_APP_ID"] = Context.config["lwa_app_id"]
    os.environ["LWA_CLIENT_SECRET"] = Context.config["lwa_client_secret"]
    os.environ["SP_API_ROLE_ARN"] = Context.config["role_arn"]
    os.environ["SP_API_ACCESS_KEY"] = Context.config["aws_access_key_id"]
    os.environ["SP_API_SECRET_KEY"] = Context.config["aws_secret_access_key"]


def get_mapped_marketplace(country_code="US"):
    markets = {
        "US": Marketplaces.US,
        "DE": Marketplaces.DE,
        "MX": Marketplaces.MX,
        "CA": Marketplaces.CA,
        "UK": Marketplaces.UK,
        "FR": Marketplaces.FR
    }

    if country_code not in markets:
        raise Exception("Wrong Country Code")

    return markets[country_code]


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    Context.tap_start = utils.now()
    Context.config = args.config
    set_env_variables()
    set_context_streams()
    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))
    # Otherwise run in sync mode
    else:
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()
        Context.state = args.state
        try:
            sync()
        except Exception as ex:
            # stop importing flag for the target
            singer.write_state({
                'stop_importing': True
            })
            raise ex


if __name__ == "__main__":
    main()
