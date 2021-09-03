from pyomnisci import *


def _deprication_warning():
    """
    Use a function here so that `os` does not get imported as a
    property of `pymapd`
    """
    import os
    disablewarning = os.environ.get('DISABLE_PYMAPD_WARNING', False)
    if not disablewarning:
        print('WARNING: pymapd is now pyomnisci, please update libraries',
              'and references as pymapd may cease to function as expected',
              'in the future. To disable this warning, set the environment',
              'variable DISABLE_PYMAPD_WARNING to a truthy value.')


_deprication_warning()
