import random
import tempfile
from typing import Type

from dffml.repo import Repo, RepoData
from dffml.model.model import ModelConfig
from dffml.source.source import Sources
from dffml.source.memory import MemorySource, MemorySourceConfig
from dffml.feature import Data, DefFeature, Features
from dffml.util.asynctestcase import AsyncTestCase

from dffml_model_scratch.model.slr import SLR, SLRConfig


FEATURE_DATA = [
    [42, 12.39999962,11.19999981],
    [43, 14.30000019,12.5],
    [44, 14.5,12.69999981],
    [45, 14.89999962,13.10000038],
    [46, 16.10000038,14.10000038],
    [47, 16.89999962,14.80000019],
    [48, 16.5,14.39999962],
    [49, 15.39999962,13.39999962],
    [50, 17,14.89999962],
    [51, 17.89999962,15.60000038],
    [52, 18.79999924,16.39999962],
    [42, 20.29999924,17.70000076],
    [42, 22.39999962,19.60000038],
    [42, 19.39999962,16.89999962],
    [42, 15.5,14],
    [42, 16.70000076,14.60000038],
    [42, 17.29999924,15.10000038],
    [42, 18.39999962,16.10000038],
    [42, 19.20000076,16.79999924],
    [42, 17.39999962,15.19999981],
    [42, 19.5,17],
    [42, 19.70000076,17.20000076],
    [42, 21.20000076,18.60000038],
]


class TestSLR(AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        cls.model_dir = tempfile.TemporaryDirectory()
        cls.model = SLR(SLRConfig(directory=cls.model_dir.name, predict='Y'))
        cls.feature = DefFeature('X', float, 1)
        cls.features = Features(DefFeature('Z', float, 1), cls.feature)
        Z, X, Y = list(zip(*FEATURE_DATA))
        cls.repos = [
            Repo(str(i),
                 data={'features': {
                     'X': X[i],
                     'Y': Y[i],
                     'Z': Z[i],
                     }}
                 )
            for i in range(0, len(Y))
            ]
        cls.sources = \
            Sources(MemorySource(MemorySourceConfig(repos=cls.repos)))

    @classmethod
    def tearDownClass(cls):
        cls.model_dir.cleanup()

    async def test_context(self):
        async with self.sources as sources, self.features as features, \
                self.model as model:
            async with sources() as sctx, model(features) as mctx:
                # Test train
                await mctx.train(sctx)
                return
                # Test accuracy
                res = await mctx.accuracy(sctx)
                self.assertTrue(0.0 <= res < 1.0)
                # Test predict
                async for repo, prediction, confidence in mctx.predict(sctx.repos()):
                    correct = FEATURE_DATA[int(repo.src_url)][1]
                    # Comparison of correct to prediction to make sure prediction is within a reasonable range
                    self.assertGreater(prediction, correct - (correct * 0.10))
                    self.assertLess(prediction, correct + (correct * 0.10))
