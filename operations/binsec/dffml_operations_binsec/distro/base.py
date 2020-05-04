import abc
from typing import AsyncIterator, Dict, NewType

PackageURL = NewType("PackageURL", str)
BinaryPath = NewType("BinaryPath", bool)


class Distro(abc.ABC):
    """
    Abstract base class which should be implemented to scan a Linux distro
    """

    @abc.abstractmethod
    async def packages(self) -> AsyncIterator[PackageURL]:
        """
        Yields URLs of packages within the most recent release of the distro.
        """

    @abc.abstractmethod
    async def report(self) -> Dict[PackageURL, Dict[BinaryPath, bool]]:
        """
        Returns a dictionary mapping package URLs to a dictionary with the keys
        being binaries and the values being a boolean for if that binary is a
        position independent executable.
        """
