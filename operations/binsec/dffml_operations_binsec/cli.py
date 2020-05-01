from dffml import CMD


class ClearLinux(CMD):
    async def run(self):
        self.logger.debug(self)


class Distro(CMD):
    clearlinux = ClearLinux


class Scan(CMD):
    distro = Distro


class BinSec(CMD):
    scan = Scan
