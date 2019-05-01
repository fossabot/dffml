import io
import os
import sys
import tarfile
import asyncio
import tempfile
from typing import Dict, Any, NamedTuple

import aiohttp
from rpmfile import RPMFile
from rpmfile.errors import RPMError

from dffml.df import op, Stage, Operation, OperationImplementation, \
    OperationImplementationContext

from dffml_feature_git.util.proc import check_output

# pylint: disable=no-name-in-module
from .definitions import URL, \
    URLBytes, \
    RPMObject, \
    rpm_filename, \
    binary, \
    binary_is_PIE

from .log import LOGGER

if sys.platform == 'win32':
    asyncio.set_event_loop(asyncio.ProactorEventLoop())

url_to_urlbytes = Operation(
    name='url_to_urlbytes',
    inputs={
        'URL': URL,
    },
    outputs={
        'download': URLBytes
    },
    conditions=[])

class URLBytesObject(NamedTuple):
    URL: str
    body: bytes

    def __repr__(self):
        return '%s(URL=%s, body=%s...)' % (self.__class__.__qualname__,
                                           self.URL, self.body[:10],)

    def __str__(self):
        return repr(self)

class URLToURLBytesContext(OperationImplementationContext):

    async def run(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        self.logger.debug('Start resp: %s', inputs['URL'])
        async with self.parent.session.get(inputs['URL']) as resp:
            return {
                'download': URLBytesObject(URL=inputs['URL'],
                                           body=await resp.read())
            }

class URLToURLBytes(OperationImplementation):

    op = url_to_urlbytes

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = None
        self.session = None

    def __call__(self,
                 ctx: 'BaseInputSetContext',
                 ictx: 'BaseInputNetworkContext') \
            -> URLToURLBytesContext:
        return URLToURLBytesContext(self, ctx, ictx)

    async def __aenter__(self) -> 'OperationImplementationContext':
        self.client = aiohttp.ClientSession(trust_env=True)
        self.session = await self.client.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self.client is not None:
            await self.client.__aexit__(exc_type, exc_value, traceback)
            self.client = None
        self.session = None

@op(inputs={
        'download': URLBytes,
    },
    outputs={
        'rpm': RPMObject
    })
async def urlbytes_to_tarfile(download: URLBytesObject):
    try:
        return {
            'rpm': tarfile.open(name=download.URL,
                                fileobj=io.BytesIO(download.body)).__enter__()
        }
    except Exception as error:
        LOGGER.debug('urlbytes_to_tarfile: Failed to instantiate '
                     'TarFile(%s): %s', download.URL, error)

@op(inputs={
        'download': URLBytes,
    },
    outputs={
        'rpm': RPMObject
    })
async def urlbytes_to_rpmfile(download: URLBytesObject):
    try:
        return {
            'rpm': RPMFile(name=download.URL,
                           fileobj=io.BytesIO(download.body)).__enter__()
        }
    except AssertionError as error:
        LOGGER.debug('urlbytes_to_rpmfile: Failed to instantiate '
                     'RPMFile(%s): %s', download.URL, error)
    except RPMError as error:
        LOGGER.debug('urlbytes_to_rpmfile: Failed to instantiate '
                     'RPMFile(%s): %s', download.URL, error)

@op(inputs={
        'rpm': RPMObject
    },
    outputs={
        'files': rpm_filename
    },
    expand=['files'])
async def files_in_rpm(rpm: RPMFile):
    return {
        'files': list(map(lambda rpminfo: rpminfo.name, rpm.getmembers()))
    }

@op(inputs={
        'rpm': RPMObject,
        'filename': rpm_filename
    },
    outputs={
        'binary': binary
    })
async def binary_file(rpm: RPMFile, filename: str):
    tempf = tempfile.NamedTemporaryFile(delete=False)
    handle = rpm.extractfile(filename)
    sig = handle.read(4)
    if len(sig) != 4 or sig != b'\x7fELF':
        return
    tempf.write(b'\x7fELF')
    tempf.write(handle.read())
    tempf.close()
    return {
        'binary': tempf.name
    }

@op(inputs={
        'binary_path': binary
    },
    outputs={
        'is_pie': binary_is_PIE
    })
async def pwn_checksec(binary_path: str):
    is_pie = False
    try:
        checksec = (await check_output('pwn', 'checksec', binary_path))\
            .split('\n')
        checksec = list(map(lambda line: line.replace(':', '')
                            .strip().split(maxsplit=1),
                            checksec))
        checksec = list(filter(bool, checksec))
        checksec = dict(checksec)
        LOGGER.debug('checksec: %s', checksec)
        is_pie = bool('enabled' in checksec['PIE'])
    except Exception as error:
        LOGGER.info('pwn_checksec: %s', error)
    return {
        'is_pie': is_pie
    }

@op(inputs={
    'rpm': RPMObject
    },
    outputs={},
    stage=Stage.CLEANUP)
async def cleanup_rpm(rpm: RPMFile):
    try:
        rpm.__exit__(None, None, None)
    except TypeError:
        rpm.__exit__()

@op(inputs={
    'binary_path': binary
    },
    outputs={},
    stage=Stage.CLEANUP)
async def cleanup_binary(binary_path: str):
    os.unlink(binary_path)
