"""
Include and merge docker-compose configurations into a single file.

Given a docker-compose.yml file, fetch each configuration in the include
section and merge it into a base docker-compose.yml. If any of the included
files have include sections continue to fetch and merge each of them until
there are no more files to include.

"""
import argparse
import logging
import sys
import os

import requests
import requests.exceptions
from six.moves.urllib.parse import urlparse

from compose_addons import version
from compose_addons.config_utils import read_config, write_config

log = logging.getLogger(__name__)


class ConfigError(Exception):
    pass


class FetchExternalConfigError(ConfigError):
    pass


def normalize_url(url):
    url = urlparse(url)
    return url if url.scheme else url._replace(scheme='file')


def get_project_from_file(url):
    # Handle urls in the form file://./some/relative/path
    old_dir = os.getcwd()
    os.chdir(os.path.dirname(url.path))
    path = url.netloc + url.path if url.netloc.startswith('.') else url.path
    with open(path, 'r') as fh:
        config = resolve_relative_paths(read_config(fh))
        os.chdir(old_dir)
        return config


# TODO: integration test for this
def get_project_from_http(url, config):
    try:
        response = requests.get(
            url.geturl(),
            timeout=config.get('timeout', 20),
            verify=config.get('verify_ssl_cert', True),
            cert=config.get('ssl_cert', None),
            proxies=config.get('proxies', None))
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise FetchExternalConfigError("Failed to include %s: %s" % (
            url.geturl(), e))
    return read_config(response.text)


# Return the connection from a function, so it can be mocked in tests
def get_boto_conn():
    # Local import so that boto is only a dependency if it's used
    import boto.s3.connection
    return boto.s3.connection.S3Connection()


def get_project_from_s3(url):
    import boto.exception
    try:
        conn = get_boto_conn()
        bucket = conn.get_bucket(url.netloc)
    except (boto.exception.BotoServerError, boto.exception.BotoClientError) as e:
        raise FetchExternalConfigError(
            "Failed to include %s: %s" % (url.geturl(), e))

    key = bucket.get_key(url.path)
    if not key:
        raise FetchExternalConfigError(
            "Failed to include %s: Not Found" % url.geturl())

    return read_config(key.get_contents_as_string())


def fetch_external_config(url, fetch_config):
    log.info("Fetching config from %s" % url.geturl())

    if url.scheme in ('http', 'https'):
        return get_project_from_http(url, fetch_config)

    if url.scheme == 'file':
        return get_project_from_file(url)

    # TODO: pass fetch_config, for timeout
    if url.scheme == 's3':
        return get_project_from_s3(url)

    raise ConfigError("Unsupported url scheme \"%s\" for %s." % (
        url.scheme,
        url))


class ConfigCache(object):
    """
    Cache each config by url. Always return a new copy of the cached dict.
    """

    def __init__(self, fetch_func):
        self.cache = {}
        self.fetch_func = fetch_func

    def get(self, url):
        if url not in self.cache:
            self.cache[url] = self.fetch_func(url)
        return dict(self.cache[url])


def apply_namespace(name, namespace, service_names):
    if name.startswith(namespace) or name not in service_names:
        return name
    return '%s.%s' % (namespace, name)


def merge_configs(base, configs):
    for config in configs:
        base.update(config)
    return base


def fetch_includes(base_config, cache, parent):
    return [fetch_include(cache, url, parent + '.' + namespace if parent else namespace) for namespace, url in base_config.pop('include', {}).iteritems()]


def fetch_include(cache, url, namespace):
    config = cache.get(normalize_url(url))
    if namespace:
        config = resolve_namespaced_links(config, namespace, config.keys())
    for key in config.keys():
        if key == 'include':
            continue
        service = config.pop(key)
        config[namespace + '.' + key] = service
    configs = fetch_includes(config, cache, None)
    return merge_configs(config, configs)


def resolve_relative_paths(config):
    for key, service in config.iteritems():
        build = service.pop('build', None)
        if build:
            build = os.path.abspath(build)
            service['build'] = build

        volumes = service.pop('volumes', [])
        for index, volume in enumerate(volumes):
            if ':' in volume:
                host = volume.split(':')[0]
                container = volume.split(':')[1]
                host = os.path.abspath(host)
                volumes[index] = host + ':' + container
        if len(volumes) > 0:
            service['volumes'] = volumes

        env_file = service.pop('env_file', [])
        for index, env in enumerate(env_file):
            env_file[index] = os.path.abspath(env)
        if len(env_file) > 0:
            service['env_file'] = env_file

        extends = service.pop('extends', None)
        if extends and extends.file:
            extends.file = os.path.abspath(extends.file)
            service.extends = extends

    return config


def resolve_namespaced_links(config, namespace, servicekeys):
    for key, service in config.iteritems():
        if key not in servicekeys:
            continue

        links = service.pop('links', [])
        for index, link in enumerate(links):
            name = link.split(':')[0]
            alias = link.split(':')[1]
            if name not in servicekeys:
                continue
            if namespace:
                name = namespace + '.' + name
            if alias:
                links[index] = name + ':' + alias
            else:
                links[index] = name
        if len(links) > 0:
            service['links'] = links

        volumes_from = service.pop('volumes_from', [])
        for index, volume in enumerate(volumes_from):
            name = volume.split(':')[0]
            if name not in servicekeys:
                continue
            alias = None
            if len(volume.split(':')) > 1:
                alias = volume.split(':')[1]
            if namespace:
                name = namespace + '.' + name
            if alias:
                volumes_from[index] = name + ':' + alias
            else:
                volumes_from[index] = name
        if len(volumes_from) > 0:
            service['volumes_from'] = volumes_from

    return config

def include(base_config, fetch_config):
    def fetch(url):
        return fetch_external_config(url, fetch_config)

    cache = ConfigCache(fetch)
    # Remove the namespace key from the base config, if it exists
    base_config.pop('namespace', None)
    return merge_configs(base_config, fetch_includes(base_config, cache, None))


def get_args(args=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', action='version', version=version)
    parser.add_argument(
        'compose_file',
        type=argparse.FileType('r'),
        default=sys.stdin,
        help="Path to a docker-compose configuration with includes.")
    parser.add_argument(
        '-o', '--output',
        type=argparse.FileType('w'),
        default=sys.stdout,
        help="Output filename, defaults to stdout.")
    # TODO: separate argument group for fetch config args
    parser.add_argument(
        '--timeout',
        help="Timeout used when making network calls.",
        type=int)

    return parser.parse_args(args=args)


# TODO: other fetch config args
def build_fetch_config(args):
    return {
        'timeout': args.timeout,
    }


def main(args=None):
    args = get_args(args=args)
    old_dir = os.getcwd()
    os.chdir(os.path.dirname(args.compose_file.name))
    config = include(read_config(args.compose_file), build_fetch_config(args))
    write_config(config, args.output)
    os.chdir(old_dir)
