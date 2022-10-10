
import os
import sys
import logging
import functools
import subprocess

log = logging.getLogger(__name__)


class IntegrityError(Exception):
    pass


class SoftError(Exception):
    pass


def handle_soft_errors(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SoftError as err:
            sys.stderr.write(str(err) + os.linesep)
            sys.stderr.flush()
    return wrapper


def humanize(size):
    units = ("M", "G", "T")
    unit_index = 0
    size = float(size) / (1024**2)  # Mega
    while size > 1024 and unit_index < (len(units) - 1):
        size = size / 1024
        unit_index += 1
    size = f"{size:.2f}"
    size = size.rstrip('0').rstrip('.')
    return f"{size} {units[unit_index]}"


def cached(func):
    @functools.wraps(func)
    def cacheing_wrapper(self, *a, **kwa):
        cache_key = func.__name__ + '_cached_value'
        if len(a) or len(kwa):
            # make sure we don't shoot ourselves in the foot by calling this on a method with args
            raise AssertionError("'cached' decorator called on method with arguments!")
        if not hasattr(self, cache_key):
            val = func(self, *a, **kwa)
            setattr(self, cache_key, val)
        return getattr(self, cache_key)
    return cacheing_wrapper


class CommandExecutor(object):
    @staticmethod
    def shell(cmd, dry_run=False, capture=False):
        if dry_run:
            print(cmd)
        else:
            try:
                if capture:
                    res = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
                    #res = subprocess.check_output(cmd, shell=True)
                else:
                    res = subprocess.check_call(cmd, shell=True)
                return res
            except subprocess.CalledProcessError as err:
                log.error(f"Shell Command Failed: {err.cmd}")
                log.error(f"Error Message: {err.stdout}")
                sys.exit(1)

    @property
    @cached
    def has_pv(self):
        return 0 == subprocess.call(
            ['which', 'pv'],
            stderr=subprocess.STDOUT, stdout=subprocess.PIPE
        )

    def pipe(self, cmd1, cmd2, quiet=False, estimated_size=None, **kwargs):
        """Executes commands"""
        if self.has_pv and not quiet:
            pv = "pv" if estimated_size is None else f"pv --size {estimated_size}"
            fullcmd = f"{cmd1} | {pv} | {cmd2}"
        else:
            fullcmd = f"{cmd1} | {cmd2}"

        log.info(fullcmd)
        return self.shell(fullcmd, **kwargs)
