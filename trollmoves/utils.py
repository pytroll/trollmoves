"""Utility functions for Trollmoves."""

import socket
from urllib.parse import urlparse, urlunparse


def clean_url(url):
    """Remove login info from *url*."""
    if isinstance(url, str):
        urlobj = urlparse(url)
    else:
        urlobj = url
    return urlunparse((urlobj.scheme, urlobj.hostname,
                       urlobj.path, "", "", ""))


def get_local_ips():
    """Get the ips of the current machine."""
    import netifaces

    inet_addrs = [netifaces.ifaddresses(iface).get(netifaces.AF_INET)
                  for iface in netifaces.interfaces()]
    ips = []
    for addr in inet_addrs:
        if addr is not None:
            for add in addr:
                ips.append(add["addr"])
    return ips


def gen_dict_extract(var, key):
    """Exctract a value from dictionary."""
    if hasattr(var, "items"):
        for k, v in var.items():
            if k == key:
                yield v
            if hasattr(v, "items"):
                for result in gen_dict_extract(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_extract(d, key):
                        yield result


def gen_dict_contains(var, key):
    """Check dictionary containing an item."""
    if hasattr(var, "items"):
        for k, v in var.items():
            if k == key:
                yield var
            if hasattr(v, "items"):
                for result in gen_dict_contains(v, key):
                    yield result
            elif isinstance(v, list):
                for d in v:
                    for result in gen_dict_contains(d, key):
                        yield result


def translate_dict_value(var, key, callback):
    """Translate dictionary values."""
    newvar = var.copy()
    if hasattr(var, "items"):
        for k, v in var.items():
            if k == key:
                newvar[key] = callback(k, v)
            elif hasattr(v, "items"):
                newvar[k] = translate_dict_value(v, key, callback)
            elif isinstance(v, list):
                newvar[k] = [translate_dict_value(d, key, callback) for d in v]
        return newvar
    else:
        return var


def translate_dict_item(var, key, callback):
    """Translate dictionary items."""
    newvar = var.copy()
    if hasattr(var, "items"):
        for k, v in var.items():
            if k == key:
                newvar = callback(var, k)
            elif hasattr(v, "items"):
                newvar[k] = translate_dict_item(v, key, callback)
            elif isinstance(v, list):
                newvar[k] = [translate_dict_item(d, key, callback) for d in v]
        return newvar
    else:
        return var


def translate_dict(var, keys, callback, **kwargs):
    """Translate dictionary."""
    try:
        newvar = var.copy()
    except AttributeError:
        import copy
        newvar = copy.copy(var)
    if hasattr(var, "items"):
        if set(var.keys()) & set(keys):
            newvar = callback(var, **kwargs)
        for k, v in newvar.items():
            if hasattr(v, "items"):
                newvar[k] = translate_dict(v, keys, callback, **kwargs)
            elif isinstance(v, list):
                newvar[k] = [translate_dict(d, keys, callback, **kwargs)
                             for d in v]
        return newvar
    else:
        return var


def is_file_local(urlobj):
    """Check that a url path is for a local file."""
    if urlobj.scheme not in ["", "file"] and socket.gethostbyname(urlobj.netloc) not in get_local_ips():
        return False

    return True
