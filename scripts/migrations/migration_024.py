#!/usr/bin/env python3
"""Condense historic post performance values (!225)"""

import sys
sys.path.insert(0, '.')

from scripts.update import condense_performance  # noqa: E402
condense_performance.main()
