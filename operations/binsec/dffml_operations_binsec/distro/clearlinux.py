import re
import os
from typing import AsyncIterator, Dict, NewType

import aiohttp
from bs4 import BeautifulSoup

from dffml import (
    run,
    DataFlow,
    Input,
    Associate,
    MemoryOrchestratorContextConfig,
)

from .base import Distro, PackageURL, BinaryPath
from ..operations import (
    url_to_urlbytes,
    urlbytes_to_tarfile,
    urlbytes_to_rpmfile,
    files_in_rpm,
    is_binary_pie,
    cleanup_rpm,
)

DATAFLOW = DataFlow.auto(
    url_to_urlbytes,
    urlbytes_to_tarfile,
    urlbytes_to_rpmfile,
    files_in_rpm,
    is_binary_pie,
    cleanup_rpm,
    Associate,
)


class ClearLinux(Distro):
    PACKAGE_LIST_URL = os.environ.get(
        "PACKAGE_LIST_URL",
        "https://download.clearlinux.org/current/x86_64/os/Packages/",
    )

    async def packages(self) -> AsyncIterator[PackageURL]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.PACKAGE_LIST_URL) as resp:
                soup = BeautifulSoup(await resp.text(), features="html.parser")
                i = 0
                for link in soup.find_all("a", href=re.compile(".rpm")):
                    i += 1
                    yield self.PACKAGE_LIST_URL + link["href"]
                    if i > 50:
                        break

    async def report(self) -> Dict[PackageURL, Dict[BinaryPath, bool]]:
        return {
            (await ctx.handle()).as_string(): results.get(
                is_binary_pie.op.outputs["is_pie"].name, {}
            )
            async for ctx, results in run(
                MemoryOrchestratorContextConfig(DATAFLOW, max_ctxs=25),
                {
                    url: [
                        Input(
                            value=url,
                            definition=url_to_urlbytes.op.inputs["URL"],
                        ),
                        Input(
                            value=[
                                is_binary_pie.op.inputs["filename"].name,
                                is_binary_pie.op.outputs["is_pie"].name,
                            ],
                            definition=Associate.op.inputs["spec"],
                        ),
                    ]
                    async for url in self.packages()
                },
            )
        }
