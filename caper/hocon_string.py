from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from pyhocon import ConfigFactory, HOCONConverter

from .dict_tool import merge_dict

logger = logging.getLogger(__name__)


NEW_LINE = '\n'
RE_HOCON_INCLUDE = [
    r'include\s+(?:required|url|file|classpath)\(.*\)',
    r'include\s+".*\.(?:conf|hocon)"',
]
RE_HOCONSTRING_INCLUDE = r'HOCONSTRING_INCLUDE_(?:.*)\s*=\s*"(?:.*)"'
RE_HOCONSTRING_INCLUDE_VALUE = r'HOCONSTRING_INCLUDE_(?:.*)\s*=\s*"(.*)"'
HOCONSTRING_INCLUDE_KEY = 'HOCONSTRING_INCLUDE_{id}'


def escape_double_quotes(double_quotes: str) -> str:
    return double_quotes.replace('"', '\\"')


def unescape_double_quotes(escaped_double_quotes: str) -> str:
    return escaped_double_quotes.replace('\\"', '"')


def is_valid_include(include: str) -> bool:
    is_valid_format = False
    for regex in RE_HOCON_INCLUDE:
        if re.findall(regex, include):
            is_valid_format = True
            break

    return is_valid_format


def get_include_key(include_str: str) -> str:
    """Use md5sum hash of the whole include statement string for a key."""
    return hashlib.md5(include_str.encode(), usedforsecurity=False).hexdigest()


def wrap_includes(hocon_str: str) -> str:
    """Convert `include` statement string into key = val format.
    Returns '{key} = "{double_quote_escaped_val}"'.
    """
    for regex in RE_HOCON_INCLUDE:
        for include in re.findall(regex, hocon_str):
            if '\\"' in include:
                continue

            logger.debug('Found include in HOCON: %s', include)

            hocon_str = hocon_str.replace(
                include,
                f'{HOCONSTRING_INCLUDE_KEY.format(id=get_include_key(include))} = "{escape_double_quotes(include)}"',
            )
    return hocon_str


def unwrap_includes(key_val_str: str) -> str | None:
    """Convert '{key} = "{val}"" formatted string to the original `include` statement string.
    Args:
        key:
            HOCONSTRING_INCLUDE_KEY with `id` as md5sum hash of the original
            `include` statement string.
        val:
            Double-quote-escaped `include` statement string.
    """
    val = re.findall(RE_HOCONSTRING_INCLUDE_VALUE, key_val_str)
    if val:
        if len(val) > 1:
            msg = f'Found multiple matches. Wrong include key=val format? {val}'
            raise ValueError(
                msg,
            )
        return unescape_double_quotes(val[0])
    return None


class HOCONString:
    def __init__(self, hocon_str: str) -> None:
        """Find an `include` statement (VALUE) in HOCON string and then convert it
        into a HOCONSTRING_INCLUDE_KEY="VALUE" pair in HOCON.

        Double-quotes will be escaped with double slashes.
        Then the VALUE is kept as it is as a value and can be recovered later when
        it is converted back to HOCON string.

        This workaround is to skip parsing `include` statements since there is no
        information about `classpath` at the parsing time and pyhocon will error out and
        will stop parsing.

        e.g. we don't know what's in `classpath` before the backend conf file is
        passed to Cromwell.
        """
        if not isinstance(hocon_str, str):
            msg = 'HOCONString() takes str type only.'
            raise TypeError(msg)

        self._hocon_str = wrap_includes(hocon_str)

    def __str__(self) -> str:
        return self.get_contents()

    @classmethod
    def from_dict(cls, d: dict[str, Any], include: str = '') -> HOCONString:
        """Create HOCONString from dict.

        Args:
            include:
                `include` statement to be added to the top of the HOCONString.
        """
        hocon = ConfigFactory.from_dict(d)
        hocon_str = HOCONConverter.to_hocon(hocon)

        if include:
            if not is_valid_include(include):
                msg = f'Wrong HOCON include format. {include}'
                raise ValueError(
                    msg,
                )
            hocon_str = NEW_LINE.join([include, hocon_str])

        return cls(hocon_str=hocon_str)

    def to_dict(self, with_include: bool = True) -> dict[str, Any]:
        """Convert HOCON string into dict.

        Args:
            with_include:
                If True then double-quote-escaped `include` statements will be kept as a plain string
                under key HOCONSTRING_INCLUDE_KEY.
                Otherwise, `include` statements will be excluded.
        """
        hocon_str = self._hocon_str if with_include else self.get_contents(with_include=False)

        c = ConfigFactory.parse_string(hocon_str)
        j = HOCONConverter.to_json(c)

        return json.loads(j)

    def merge(
        self,
        b: HOCONString | dict[str, Any] | str,
        update: bool = False,
    ) -> str:
        """Merge self with b and then returns a plain string of merged.
        Args:
            b:
                HOCONString, dict, str to be merged.
                b's `include` statement will always be ignored.
            update:
                If True then replace self with a merged one.
        Returns:
            String of merged HOCONs.
        """
        if isinstance(b, HOCONString):
            d = b.to_dict()
        elif isinstance(b, str):
            d = HOCONString(b).to_dict()
        elif isinstance(b, dict):
            d = b
        else:
            msg = f'Unsupported type {type(b)}'
            raise TypeError(msg)

        self_d = self.to_dict()
        merge_dict(self_d, d)

        hocon = ConfigFactory.from_dict(self_d)

        hocon_str = HOCONConverter.to_hocon(hocon)
        if update:
            self._hocon_str = hocon_str

        return HOCONString(hocon_str).get_contents()

    def get_contents(self, with_include: bool = True) -> str:
        """Check if `include` statement is stored as a plain string.
        If exists, converts it back to HOCON `include` statement.

        Args:
            with_include: (renamed/changed from without_include)
                If True then recover all includes statements from include key=val form
                (RE_HOCONSTRING_INCLUDE).
                Otherwise, excludes all `include` statements.
        """
        hocon_str = self._hocon_str

        for include_key_val in re.findall(RE_HOCONSTRING_INCLUDE, self._hocon_str):
            logger.debug(
                'Found include key in HOCONString: %s',
                include_key_val,
            )
            if with_include:
                original_include_str = unwrap_includes(include_key_val)
                if original_include_str:
                    hocon_str = hocon_str.replace(include_key_val, original_include_str)
            else:
                hocon_str = hocon_str.replace(include_key_val, '')

        return hocon_str
