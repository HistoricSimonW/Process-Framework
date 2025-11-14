
class EarlyEscape(Exception):
    """ base class for managed, expected `Exception`s, describing, for instance, a process terminating early by design """
    ...


class NoChangesToUpdate(EarlyEscape):
    """ end the run early; there are no changes to update """
    ...