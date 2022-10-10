import os
import logging
import configparser

log = logging.getLogger(__name__)

_settings = None
_onion_dict_guard = object()

config_defaults = {
    "PROFILE" : "default",
    "ENDPOINT" : "aws",
    "STORAGE_CLASS" : "STANDARD_IA",
    "S3_PREFIX" : "zfs3backup/",
    "SNAPSHOT_PREFIX" : "auto",
    "COMPRESSOR" : "pigz1",
    "ENCRYPTOR" : "none",
    "CONCURRENCY" : "4",
    "MAX_RETRIES" : "3",
    "CHUNK_SIZE" : "256M",
    "KEEP_NUM_FULL_SNAPSHOTS" : "2",
}


class OnionDict(object):
    """Wraps multiple dictionaries. Tries to read data from each dict
    in turn.
    Used to implement a fallback mechanism.
    """
    def __init__(self, dictionaries, sections=None):
        self._dictionaries = dictionaries
        self._sections = sections or {}

    def __repr__(self):
        return f"{self.__class__.__name__}(dictionaries={repr(self._dictionaries)}, sections={repr(self._sections)})"

    def _get(self, key, section=None, default=_onion_dict_guard):
        """Try to get the key from each dict in turn.
        If you specify the optional section it looks there first.
        """
        if section is not None:
            section_dict = self._sections.get(section, {})
            if key in section_dict:
                return section_dict[key]
        for d in self._dictionaries:
            if key in d:
                return d[key]
        if default is _onion_dict_guard:
            raise KeyError(key)
        else:
            return default

    def __contains__(self, key):
        for d in self._dictionaries:
            if key in d:
                return True
        return False

    def __getitem__(self, key):
        return self._get(key)

    def get(self, key, *, default=None, section=None):
        return self._get(key, section=section, default=default)


def get_config(config=None, args=None):
    global _settings
    if _settings is None:
        _config = configparser.ConfigParser()
        # Start with the installation directory
        # default = os.path.join(zfs3backup.__path__[0], "zfs3backup.conf")
        # _config.read(default)
        if os.environ.get('SKIP_CONFIG_FILE', 'false').lower() != 'true':
            if config is None:
                fname = "/etc/zfs3backup/zfs3backup.conf"
            else:
                fname = config
            log.info(f"Loading config: {fname}")
            _config.read(fname)

        layers = [
            args if args is not None else {},
            os.environ,
            dict((k.upper(), v) for k, v in _config.items("main")),
            config_defaults,
        ]
        sections = {}
        for section in _config.sections():
            if section != 'main':
                section_dict = dict(
                    (k.upper(), v)
                    for k, v in _config.items(section)
                )
                sections[section] = section_dict
        _settings = OnionDict(layers, sections)

    return _settings
