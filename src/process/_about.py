import importlib.metadata as metadata

def get_app_version() -> str:
    """ try to get the version of this package, falling back to 0 (if, for example, we've not been built yet) """
    version_str = "0"
    try:
        from ._version import version # type: ignore
        if isinstance(version, str):
            return version
        
    except Exception:
        pass
    
    return version_str


def get_dist_name() -> str:
    """ get the name of the running distribution """
    top_package = __name__.split('.')[0]
    return (metadata.packages_distributions().get(top_package, [top_package]) or [top_package])[0]


def get_app_name() -> str:
    """ get the name of the running app """
    try:
        return metadata.distribution(get_dist_name()).metadata["Name"]
    except metadata.PackageNotFoundError:
        return 'package-not-found'


def safe_version(dist: str) -> str:
    """ get the version of a distribution, returning "0" if it is unavailable """
    try:
        return metadata.version(dist)
    except Exception:
        return "0"
    

def get_process_model_version() -> str:
    """ get the version of the `preocess_model` used by this package """
    return safe_version("process_model")


__app_version__ = get_app_version()
__app_name__ = get_app_name()
__process_model_version__ = get_process_model_version()


if __name__ == '__main__':
    print(__app_version__)
    print(__app_name__)
    print(__process_model_version__)