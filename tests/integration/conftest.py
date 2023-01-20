#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# Apache Software License, version 2.0

"""Configure integration test run."""

import pathlib

import pytest
from pytest_operator.plugin import OpsTest

from helpers import ETCD, NHC, SINGULARITY_DEB, SINGULARITY_RPM, VERSION


@pytest.fixture(scope="module")
async def slurmctld_charm(ops_test: OpsTest):
    """Build slurmctld charm to use for integration tests."""
    charm = await ops_test.build_charm(".")
    return charm


def pytest_sessionfinish(session, exitstatus) -> None:
    """Clean up repository after test session has completed."""
    pathlib.Path(ETCD).unlink(missing_ok=True)
    pathlib.Path(NHC).unlink(missing_ok=True)
    pathlib.Path(SINGULARITY_DEB).unlink(missing_ok=True)
    pathlib.Path(SINGULARITY_RPM).unlink(missing_ok=True)
    pathlib.Path(VERSION).unlink(missing_ok=True)
