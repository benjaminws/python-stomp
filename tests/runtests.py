#!/usr/bin/env python

import sys

import nose


if __name__ == '__main__':
    nose_args = sys.argv + [r'-m',
                            r'((?:^|[b_.-])(:?[Tt]est|When|should))',
                            r'--with-coverage',
                            r'--cover-package=stomp',
                            r'--cover-erase']
    nose.run(argv=nose_args)
