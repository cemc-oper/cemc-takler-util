from typing import Optional, Literal, Union

from takler.core.node import Node
from takler.tasks.shell.constant import DEFAULT_TAKLER_SHELL_JOB_CMD, DEFAULT_TAKLER_SHELL_KILL_CMD

from pydantic import BaseModel


class RuntimeConfig(BaseModel):
    runtime_type: Literal["shell", "slurm"]
    job_type: Literal["serial", "parallel"]
    job_class: Optional[str] = None
    nodes: Optional[int] = None
    tasks_per_node: Optional[int] = None
    workload_key: Optional[str] = None


slurm_job_cmd = "sbatch {{ TAKLER_JOB }}"
slurm_kill_cmd = "scancel {{ TAKLER_RID }}"


def set_runtime(node: Node, runtime_config: RuntimeConfig):
    """
    set runtime variables for some node.
    """
    runtime_type = runtime_config.runtime_type
    if runtime_type == "shell":
        node.add_parameter(shell_job())
    elif runtime_type == "slurm":
        job_type = runtime_config.job_type
        if job_type == "serial":
            class_name = runtime_config.job_class
            workload_key = runtime_config.workload_key
            node.add_parameter(slurm_serial_job(class_name=class_name, workload_key=workload_key))
        elif job_type == "parallel":
            nodes = runtime_config.nodes
            tasks_per_node = runtime_config.tasks_per_node
            class_name = runtime_config.job_class
            workload_key = runtime_config.workload_key
            node.add_parameter(slurm_parallel_job(
                nodes=nodes,
                tasks_per_node=tasks_per_node,
                class_name=class_name,
                workload_key=workload_key
            ))
        else:
            raise ValueError(f"job type for slurm is not supported: {job_type}")
    else:
        raise ValueError(f"runtime type is not supported: {runtime_type}")


def slurm_serial_job(class_name: str = "serial", workload_key: Optional[str] = None) -> dict:
    params = dict(
        TAKLER_SHELL_JOB_CMD=slurm_job_cmd,
        TAKLER_SHELL_KILL_CMD=slurm_kill_cmd,
        PARTITION=class_name,
    )
    if workload_key is not None:
        params["WCKEY"] = workload_key
    return params


def slurm_parallel_job(
        nodes: int, tasks_per_node: int = 32, class_name: str = "normal", workload_key: Optional[str] = None):
    params = dict(
        TAKLER_SHELL_JOB_CMD=slurm_job_cmd,
        TAKLER_SHELL_KILL_CMD=slurm_kill_cmd,
        PARTITION=class_name,
        NODES=nodes,
        TASKS_PER_NODE=tasks_per_node,
    )
    if workload_key is not None:
        params["WCKEY"] = workload_key
    return params


def shell_job():
    return dict(
        TAKLER_SHELL_JOB_CMD=DEFAULT_TAKLER_SHELL_JOB_CMD,
        TAKLER_SHELL_KILL_CMD=DEFAULT_TAKLER_SHELL_KILL_CMD
    )
