#!/usr/bin/env python3
# Copyright 2023 Canonical Ltd.
# Apache Software License, version 2.0

"""Helpers for the slurmctld integration tests."""

import contextlib
import logging
import pathlib
import shlex
import subprocess
import tempfile
from io import StringIO
from typing import Dict

import paramiko
import tenacity
from pytest_operator.plugin import OpsTest
from urllib import request

logger = logging.getLogger(__name__)

ETCD = "etcd-v3.5.0-linux-amd64.tar.gz"
ETCD_URL = f"https://github.com/etcd-io/etcd/releases/download/v3.5.0/{ETCD}"
NHC = "lbnl-nhc-1.4.3.tar.gz"
NHC_URL = f"https://github.com/mej/nhc/releases/download/1.4.3/{NHC}"
SINGULARITY_DEB = "singularity-ce_3.10.2-focal_amd64.deb"
SINGULARITY_DEB_URL = (
    f"https://github.com/sylabs/singularity/releases/download/v3.10.2/{SINGULARITY_DEB}"
)
SINGULARITY_RPM = "singularity-ce-3.10.2-1.el7.x86_64.rpm"
SINGULARITY_RPM_URL = (
    f"https://github.com/sylabs/singularity/releases/download/v3.10.2/{SINGULARITY_RPM}"
)
VERSION = "version"
VERSION_NUM = subprocess.run(
    shlex.split("git describe --always"), stdout=subprocess.PIPE, text=True
).stdout.strip("\n")


def get_slurmctld_res() -> Dict[str, pathlib.Path]:
    """Get slurmctld resources needed for charm deployment."""
    if not (version := pathlib.Path(VERSION)).exists():
        logger.info(f"Setting resource {VERSION} to value {VERSION_NUM}...")
        version.write_text(VERSION_NUM)
    if not (etcd := pathlib.Path(ETCD)).exists():
        logger.info(f"Getting resource {ETCD} from {ETCD_URL}...")
        request.urlretrieve(ETCD_URL, etcd)

    return {"etcd": etcd}


def get_slurmd_res() -> Dict[str, pathlib.Path]:
    """Get slurmd resources needed for charm deployment."""
    if not (nhc := pathlib.Path(NHC)).exists():
        logger.info(f"Getting resource {NHC} from {NHC_URL}...")
        request.urlretrieve(NHC_URL, nhc)
    if not (singularity_deb := pathlib.Path(SINGULARITY_DEB)).exists():
        logger.info(f"Getting resource {SINGULARITY_DEB} from {SINGULARITY_DEB_URL}...")
        request.urlretrieve(SINGULARITY_DEB_URL, singularity_deb)
    if not (singularity_rpm := pathlib.Path(SINGULARITY_RPM)).exists():
        logger.info(f"Getting resource {SINGULARITY_RPM} from {SINGULARITY_RPM_URL}...")
        request.urlretrieve(SINGULARITY_RPM_URL, singularity_rpm)

    return {"nhc": nhc, "singularity-deb": singularity_deb, "singularity-rpm": singularity_rpm}


@contextlib.asynccontextmanager
async def unit_connection(
    ops_test: OpsTest, application: str, target_unit: str
) -> paramiko.SSHClient:
    """Asynchronous context manager for connecting to a Juju unit via SSH.

    Args:
        ops_test (OpsTest): Utility class for charmed operators.
        application (str): Name of application target unit belongs to. e.g. slurmdbd
        target_unit (str): Name of unit to connect to via ssh. e.g. slurmdbd/0

    Yields:
        (paramiko.SSHClient): Open SSH connection to target unit. Connection is
            closed after context manager exits.

    Notes:
        Paramiko may fail to establish an ssh connection with the target Juju unit on
        the first try, so tenacity is used to reattempt the connection for 60 seconds.
        This to do with a delay in the public key being ready inside the unit.
    """

    @tenacity.retry(
        wait=tenacity.wait.wait_exponential(multiplier=2, min=1, max=30),
        stop=tenacity.stop_after_attempt(10),
        reraise=True,
    )
    def _connect(ssh_client: paramiko.SSHClient, **kwargs) -> None:
        """Establish SSH connection to Juju unit."""
        ssh_client.connect(**kwargs)

    with tempfile.TemporaryDirectory() as _:
        logger.info("Setting up private/public ssh keypair...")
        private_key_path = pathlib.Path(_).joinpath("id")
        public_key_path = pathlib.Path(_).joinpath("id.pub")
        subprocess.check_call(
            shlex.split(
                f"ssh-keygen -f {str(private_key_path)} -t rsa -N '' -q -C Juju:juju@localhost"
            )
        )
        await ops_test.model.add_ssh_key("ubuntu", (pubkey := public_key_path.read_text()))
        # Verify that public key is available in Juju model otherwise ssh connection may fail.
        for response in (await ops_test.model.get_ssh_keys(raw_ssh=True))["results"]:
            assert pubkey.strip("\n") in response["result"]
        pkey = paramiko.RSAKey.from_private_key(StringIO(private_key_path.read_text()))

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    for unit in ops_test.model.applications[application].units:
        if unit.name == target_unit:
            logger.info(f"Opening ssh connection to unit {target_unit}...")
            _connect(
                ssh, hostname=str(await unit.get_public_address()), username="ubuntu", pkey=pkey
            )
    yield ssh
    logger.info(f"Closing ssh connection to unit {target_unit}...")
    ssh.close()
