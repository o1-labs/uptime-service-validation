import logging
import sys
from kubernetes import client
import os
from datetime import datetime
import subprocess
import time

# Set environment variables
# Get the values of your-image and tag from environment variables
worker_image = os.environ.get("WORKER_IMAGE", "busybox")
worker_tag = os.environ.get("WORKER_TAG", "latest")


def setUpValidatorPods(time_intervals, jobs, logging, worker_image, worker_tag):
    # Create a Kubernetes API client
    api = client.BatchV1Api()
    for index, mini_batch in enumerate(time_intervals):
        job_name = f"zk-validator-{datetime.now().strftime('%y-%m-%d-%H-%M')}-{index}"  # ZKValidator
        jobs.append(job_name)
        job = client.V1Job(
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        containers=[
                            client.V1Container(
                                name="zk-validator",
                                image=f"{worker_image}:{worker_tag}",
                                command=["sleep", "10"],
                                env=[
                                    client.V1EnvVar(
                                        name="JobName", value=f"{job_name}"
                                    ),
                                    client.V1EnvVar(
                                        name="BatchStart",
                                        value=f"{mini_batch[index][0]}",
                                    ),
                                    client.V1EnvVar(
                                        name="BatchEnd", value=f"{mini_batch[index][1]}"
                                    ),
                                ],
                                image_pull_policy="Always",  # Set the image pull policy here
                            )
                        ],
                        restart_policy="Never",
                    )
                )
            ),
        )
        namespace = open(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
        ).read()
        api.create_namespaced_job(namespace, job)

        logging.info(f"Launching pod {index}")

    while True:
        for job_name in jobs:
            pod = api.read_namespaced_job(job_name, namespace)
            if pod.status.succeeded is not None and pod.status.succeeded > 0:
                logging.info(f"Pod {index} completed at {datetime.now()}")
                jobs.remove(job_name)
        if len(jobs) == 0:
            break


def setUpValidatorProcesses(
    time_intervals, logging, worker_image, worker_tag, aws_keyspace
):
    processes = []
    for index, mini_batch in enumerate(time_intervals):
        process_name = (
            f"local-validator-{datetime.now().strftime('%y-%m-%d-%H-%M')}-{index}"
        )
        command = [
            "docker",
            "run",
            "-it",
            "--rm",
            "-v",
            "~/home/piotr/cassandra/cqlshrc:/root/.cassandra/cqlshrc",
            f"{worker_image}:{worker_tag}",
            "/bin/delegation-verify",
            "cassandra",
            "--keyspace",
            aws_keyspace,
            f'"{mini_batch[0]}"',
            f'"{mini_batch[1]}"',
        ]
        print(" ".join(command))

        # Set up environment variables for the process
        env = os.environ.copy()
        # env["JobName"] = process_name
        # env["BatchStart"] = str(mini_batch[0])
        # env["BatchEnd"] = str(mini_batch[1])

        # Spawn the process
        proc = subprocess.Popen(command, env=env)
        processes.append((process_name, proc))
        logging.info(f"Launching process {index}")

    # Monitor the processes
    while processes:
        for process_name, proc in processes:
            if proc.poll() is not None:  # Check if the process has completed
                logging.info(f"Process {process_name} completed at {datetime.now()}")
                processes.remove((process_name, proc))

        time.sleep(1)  # Avoid busy waiting

        if not processes:
            break


# Usage Example
if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    time_intervals = [
        (
            datetime(2023, 11, 6, 15, 35, 47, 630499),
            datetime(2023, 11, 6, 15, 36, 17, 630499),
        ),
        (
            datetime(2023, 11, 6, 15, 36, 17, 630499),
            datetime(2023, 11, 6, 15, 36, 47, 630499),
        ),
        (
            datetime(2023, 11, 6, 15, 36, 47, 630499),
            datetime(2023, 11, 6, 15, 37, 17, 630499),
        ),
        (
            datetime(2023, 11, 6, 15, 37, 17, 630499),
            datetime(2023, 11, 6, 15, 37, 47, 630499),
        ),
        (
            datetime(2023, 11, 6, 15, 37, 47, 630499),
            datetime(2023, 11, 6, 15, 38, 17, 630499),
        ),
    ]
    setUpValidatorProcesses(
        time_intervals,
        logging,
        worker_image="mina-delegation-verify",
        worker_tag="rriy11hxzl1rlh3ms8cd5ygkzlhfmpb3",
        aws_keyspace="bpu_integration_dev",
    )
