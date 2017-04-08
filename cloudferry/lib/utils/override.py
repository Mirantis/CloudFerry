# Copyright (c) 2016 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the License);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an AS IS BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and#
# limitations under the License.

import logging
import yaml

from cloudferry.lib.base import exception

ABSENT = object()
LOG = logging.getLogger(__name__)


class InvalidOverrideConfigError(exception.AbortMigrationError):
    pass


class OverrideRule(object):
    def __init__(self, attribute, rule):
        sorted_keys = sorted(rule)
        if sorted_keys == ['default']:
            self.predicate = lambda x: True
            self.value = rule['default']
        elif sorted_keys == ['replace', 'when']:
            self._validate_match(attribute, rule['when'])
            self.predicate = self._make_predicate(attribute, rule['when'])
            self.value = rule['replace']
        else:
            raise TypeError(
                'Invalid rule, only {"replace": ..., "when": ...} or '
                '{"default": ...} are allowed: %s' % repr(rule))

    @staticmethod
    def _make_predicate(attribute, match):
        def predicate(obj):
            for key, value in match.items():
                if obj.get(key) not in value:
                    return False
            return True

        if isinstance(match, dict):
            OverrideRule._validate_match(attribute, match)
            return predicate
        else:
            return lambda x: x.get(attribute) == match

    @staticmethod
    def _validate_match(attribute, match):
        if not isinstance(match, dict):
            return
        for value in match.values():
            if not isinstance(value, list):
                raise TypeError(
                    'Invalid match rule for "%s" attribute, must be '
                    '{"attr": [...], "other_attr": [...]}: %s' %
                    (attribute, repr(match)))


class OverrideProxy(object):
    def __init__(self, obj, overrides):
        self._obj = obj
        self._overrides = overrides
        self._cache = {}

    def __getattr__(self, name):
        for rule in self._overrides.get(name, []):
            if rule.predicate(self._obj):
                return rule.value
        return getattr(self._obj, name)


def get_filename_from_stream(stream):
    return getattr(stream, 'name', 'stream')


class AttributeOverrides(object):
    ALLOWED_OBJECT_TYPES = ['volumes', 'servers']

    def __init__(self, mapping):
        self.mapping = {}
        for attr, rules in mapping.items():
            if isinstance(rules, list):
                self.mapping[attr] = [OverrideRule(attr, rule)
                                      for rule in rules]
            else:
                raise TypeError(
                    'Invalid value for attribute "%s" rules (must be '
                    'list): %s' % (attr, repr(rules)))

    @classmethod
    def from_stream(cls, stream, object_type):
        data = yaml.load(stream)

        if data is None:
            return cls.zero()

        if not isinstance(data, dict):
            raise TypeError('%s root object must be dictionary!' %
                            (get_filename_from_stream(stream),))
        object_data = data.get(object_type, {})

        if not set(data.keys()).issubset(set(cls.ALLOWED_OBJECT_TYPES)):
            raise InvalidOverrideConfigError(
                "Mapping file '%s' has unsupported entries: '%s'. "
                "Please make sure you follow template config." % (
                    get_filename_from_stream(stream), data.keys()))

        if not isinstance(object_data, dict):
            raise TypeError('%s object in mapping file %s must be '
                            'dictionary!' % (object_type,
                                             get_filename_from_stream(stream)))
        if not object_data:
            return cls.zero()

        return cls(object_data)

    @classmethod
    def from_filename(cls, mapping_filename, object_type):
        """
        Create AttributeOverrides based on YAML file
        :param mapping_filename: YAML config path
        :param object_type: The type of objects
        :return: AttributeOverrides instance
        """
        if mapping_filename is None:
            return cls.zero()

        with open(mapping_filename, 'r') as f:
            return cls.from_stream(f, object_type)

    @classmethod
    def zero(cls):
        """
        Return empty AttributeOverrides instance
        :return: empty AttributeOverrides instance
        """
        return cls({})

    def get_attr(self, obj, attribute, default=ABSENT):
        """
        Return object attribute based on overriding rules
        :param obj: some object
        :param attribute: attribute name
        :return: original or overriden attribute based on override rules
        """
        original_value = obj.get(attribute)
        for rule in self.mapping.get(attribute, []):
            if rule.predicate(obj):
                new_value = rule.value
                LOG.debug("Value of '%s' attribute has been overridden "
                          "from '%s' to the new value: '%s'",
                          attribute, original_value, new_value)
                return new_value
        if default is not ABSENT:
            LOG.debug("Value of '%s' attribute has been overridden "
                      "from '%s' to the default value: '%s'",
                      attribute, original_value, default)
            return default
        else:
            return original_value
