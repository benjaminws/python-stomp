#!/usr/bin/env python
from unittest import TestCase
import stompy.distmeta as dm

class WhenCheckingDistMeta(TestCase):
   
    def should_get_stable_release(self):
        assert isinstance(dm.is_stable_release(), type(True))

    def should_return_version_with_meta(self):
        dm.VERSION = (0, 2, 0) 
        assert isinstance(dm.version_with_meta(), type(""))
