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


# Format datetime such as it is accepted by the stateless validator
def datetime_formatter(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-5] + "+0000"


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


def setUpValidatorProcesses(time_intervals, logging, worker_image, worker_tag):
    processes = []
    for index, mini_batch in enumerate(time_intervals):
        process_name = (
            f"local-validator-{datetime.now().strftime('%y-%m-%d-%H-%M')}-{index}"
        )
        image = f"{worker_image}:{worker_tag}"
        command = [
            "docker",
            "run",
            "--privileged",
            "-it",
            "--network",
            "host",
            "--rm",
            "-v",
            f"{os.environ.get('SSL_CERTFILE')}:/var/ssl/ssl-cert.crt",
            "--env",
            "CASSANDRA_HOST",
            "--env",
            "CASSANDRA_PORT",
            "--env",
            "CASSANDRA_USERNAME",
            "--env",
            "CASSANDRA_PASSWORD",
            "--env",
            "SSL_CERTFILE=/var/ssl/ssl-cert.crt",
            "--env",
            "CASSANDRA_USE_SSL=1",
            image,
            "/bin/delegation-verify",
            "cassandra",
            "--keyspace",
            os.environ.get("AWS_KEYSPACE"),
            f"{datetime_formatter(mini_batch[0])}",
            f"{datetime_formatter(mini_batch[1])}",
            "--no-check",
        ]
        # command = [
        #     "delegation-verify",
        #     "cassandra",
        #     "--keyspace",
        #     os.environ.get("AWS_KEYSPACE"),
        #     f"{datetime_formatter(mini_batch[0])}",
        #     f"{datetime_formatter(mini_batch[1])}",
        # ]
        cmd_str = " ".join(command)

        # Set up environment variables for the process
        env = os.environ.copy()
        env["JobName"] = process_name
        env["BatchStart"] = str(mini_batch[0])
        env["BatchEnd"] = str(mini_batch[1])

        # Spawn the process
        proc = subprocess.Popen(command, env=env)
        processes.append((process_name, proc))
        logging.info(f"Launching process {index}: {cmd_str}")

    # Monitor the processes
    while processes:
        for process_name, proc in processes:
            if proc.poll() is not None:  # Check if the process has completed
                logging.info(f"Process {process_name} completed at {datetime.now()}")
                processes.remove((process_name, proc))

        time.sleep(1)

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
            datetime(2023, 11, 14, 14, 35, 47, 630),
            datetime(2023, 11, 14, 14, 36, 17, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 36, 17, 630),
            datetime(2023, 11, 14, 14, 36, 47, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 36, 47, 630),
            datetime(2023, 11, 14, 14, 37, 17, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 37, 17, 630),
            datetime(2023, 11, 14, 14, 37, 47, 630),
        ),
        (
            datetime(2023, 11, 14, 14, 37, 47, 630),
            datetime(2023, 11, 14, 14, 38, 17, 630),
        ),
    ]
    setUpValidatorProcesses(
        time_intervals,
        logging,
        worker_image,
        worker_tag,
    )
