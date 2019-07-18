import logging
import math
import os
import time

import pytest
from retrying import retry

from assertpy import assert_that
from benchmarks.common.util import get_instance_vcpus
from remote_command_executor import RemoteCommandExecutor
from tests.common.schedulers_common import get_scheduler_commands
from time_utils import minutes, seconds

NPB_BIN = "NPB3.3.1/NPB3.3-MPI/bin"


@pytest.mark.schedulers(["slurm", "sge", "torque"])
@pytest.mark.oss(["centos7", "alinux", "ubuntu1604"])
@pytest.mark.instances(["c5n.18xlarge", "i3en.24xlarge", "p3dn.24xlarge"])
def test_efa_npb_benchmarks(
    region, instance, os, scheduler, request, test_datadir, pcluster_config_reader, clusters_factory
):
    cluster_config = pcluster_config_reader(cluster_size=10)
    cluster = clusters_factory(cluster_config)
    remote_command_executor = RemoteCommandExecutor(cluster)
    slots_per_instance = get_instance_vcpus(region, instance)
    scheduler_commands = get_scheduler_commands(scheduler, remote_command_executor)

    _setup_npb(remote_command_executor, test_datadir)
    job_ids, benchmarks = _run_npb(
        remote_command_executor, scheduler_commands, test_datadir, slots_per_instance, scheduler
    )
    _wait_all_jobs_completed(remote_command_executor, job_ids)
    # give it some extra time to flush jobs stdout to files
    time.sleep(60)
    _write_results_to_outdir(request, remote_command_executor, benchmarks)


def _setup_npb(remote_command_executor, test_datadir):
    remote_command_executor.run_remote_script(
        str(test_datadir / "npb/npb_setup.sh"),
        additional_files=[str(test_datadir / "npb/make.def"), str(test_datadir / "npb/suite.def")],
    )


def _run_npb(remote_command_executor, scheduler_commands, test_datadir, slots_per_instance, scheduler):
    executables = remote_command_executor.run_remote_command("ls -1 {0}".format(NPB_BIN)).stdout.splitlines()
    assert_that(executables).is_not_empty()
    job_ids = []
    benchmarks = []
    for exe in executables:
        alg, clazz, cores = exe.split(".")
        required_nodes = math.ceil(float(cores) / slots_per_instance)
        if scheduler == "torque":
            kwargs = {"nodes": required_nodes, "slots": slots_per_instance}
        else:
            kwargs = {"slots": required_nodes * slots_per_instance}
        result = scheduler_commands.submit_script(
            str(test_datadir / "npb/mpi_submit_npb.sh"), script_args=["{0}/{1}".format(NPB_BIN, exe), cores], **kwargs
        )
        job_id = scheduler_commands.assert_job_submitted(result.stdout)
        job_ids.append(job_id)
        benchmarks.append(exe)
        logging.info("Submitted benchmark {0}-{1}-{2} as job {3}".format(alg, clazz, cores, job_id))

    return job_ids, benchmarks


@retry(retry_on_result=lambda result: result is not True, wait_fixed=seconds(10), stop_max_delay=minutes(15))
def _wait_all_jobs_completed(remote_command_executor, job_ids):
    result = remote_command_executor.run_remote_command("ls /shared/npb_results | wc -l")
    completed_jobs_count = int(result.stdout.strip())
    return completed_jobs_count == len(job_ids)


def _write_results_to_outdir(request, remote_command_executor, benchmarks):
    for benchmark in benchmarks:
        out_dir = "{out_dir}/benchmarks/{test_dir}".format(
            out_dir=request.config.getoption("output_dir"), test_dir=request.node.nodeid.replace("::", "-")
        )
        os.makedirs(out_dir, exist_ok=True)
        with open("{0}/{1}.out".format(out_dir, benchmark), "w") as file:
            file.write(
                remote_command_executor.run_remote_command("cat /shared/npb_results/{0}.out".format(benchmark)).stdout
            )
