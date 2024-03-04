import logging
import socket
import sys
from kubernetes import client, config
import os
from datetime import datetime, timezone
import subprocess
import time
import socket


def bool_env_var_set(env_var_name):
    """
    Checks if an environment variable is set and is set to a truthy value.

    :param env_var_name: The name of the environment variable.
    :return: True if the environment variable is set and is set to a truthy value.
    """
    env_var = os.environ.get(env_var_name)
    return env_var is not None and env_var.lower() in ["true", "1"]


def try_get_hostname_ip(hostname, logger, max_retries=5, initial_wait=0.2):
    """
    Attempts to resolve a hostname to an IP address with retries.

    :param hostname: The hostname to resolve.
    :param logger: The logging object.
    :param max_retries: Maximum number of retries.
    :param initial_wait: Initial wait time in seconds for the first retry.
    :return: The resolved IP address or the original hostname if resolution fails.
    """
    retry_wait = initial_wait
    for i in range(max_retries):
        try:
            ip_address = socket.gethostbyname(hostname)
            return ip_address
        except socket.gaierror as e:
            logger.warning(
                f"Attempt {i + 1}: DNS resolution for {hostname} failed: {e}. Retrying in {retry_wait} seconds..."
            )
            time.sleep(retry_wait)
            retry_wait *= 2  # Exponential backoff

    logger.error(
        f"Max retries ({max_retries}) reached. Returning the original hostname: {hostname}"
    )
    return hostname


# Format datetime such as it is accepted by the stateless validator
def datetime_formatter(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-5] + "+0000"


def setUpValidatorPods(time_intervals, logging, worker_image, worker_tag):
    # Configuring Kubernetes client
    config.load_incluster_config()

    api_core = client.CoreV1Api()
    api_batch = client.BatchV1Api()

    namespace = (
        open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read().strip()
    )

    platform = os.environ.get("PLATFORM")
    network_name = os.environ.get("NETWORK_NAME")

    service_account_name = f"delegation-verify"

    worker_cpu_request = os.environ.get("WORKER_CPU_REQUEST")
    worker_memory_request = os.environ.get("WORKER_MEMORY_REQUEST")
    worker_cpu_limit = os.environ.get("WORKER_CPU_LIMIT")
    worker_memory_limit = os.environ.get("WORKER_MEMORY_LIMIT")

    ttl_seconds = int(os.environ.get("WORKER_TTL_SECONDS_AFTER_FINISHED"))

    # List to keep track of job names
    jobs = []
    cassandra_ip = try_get_hostname_ip(os.environ.get("CASSANDRA_HOST"), logging)
    if bool_env_var_set("NO_CHECKS"):
        logging.info("stateless-verifier will run with --no-checks flag")
    for index, mini_batch in enumerate(time_intervals):

        # Job name
        job_name = f"delegation-verify-{datetime.now(timezone.utc).strftime('%y-%m-%d-%H-%M')}-{index}"

        # Define the environment variables
        env_vars = [
            client.V1EnvVar(
                name="AWS_ROLE_SESSION_NAME",
                value=job_name,
            ),
            client.V1EnvVar(
                name="AWS_REGION",
                value=os.environ.get("AWS_REGION"),
            ),
            client.V1EnvVar(
                name="AWS_DEFAULT_REGION",
                value=os.environ.get("AWS_REGION"),
            ),
            client.V1EnvVar(
                name="AWS_KEYSPACE",
                value=os.environ.get("AWS_KEYSPACE"),
            ),
            client.V1EnvVar(
                name="AWS_S3_BUCKET",
                value=os.environ.get("AWS_S3_BUCKET"),
            ),
            client.V1EnvVar(
                name="CASSANDRA_HOST",
                value=cassandra_ip,
            ),
            client.V1EnvVar(
                name="CASSANDRA_PORT",
                value=os.environ.get("CASSANDRA_PORT"),
            ),
            client.V1EnvVar(
                name="CASSANDRA_USERNAME",
                value=os.environ.get("CASSANDRA_USERNAME"),
            ),
            client.V1EnvVar(
                name="CASSANDRA_PASSWORD",
                value=os.environ.get("CASSANDRA_PASSWORD"),
            ),
            client.V1EnvVar(name="CASSANDRA_USE_SSL", value="1"),
            client.V1EnvVar(
                name="SSL_CERTFILE",
                value=os.environ.get("SSL_CERTFILE"),
            ),
            client.V1EnvVar(
                name="CQLSH",
                value=os.environ.get("CQLSH"),
            ),
            client.V1EnvVar(
                name="AUTH_VOLUME_MOUNT_PATH",
                value=os.environ.get("AUTH_VOLUME_MOUNT_PATH"),
            ),
            client.V1EnvVar(
                name="NETWORK_NAME",
                value=os.environ.get("NETWORK_NAME"),
            ),
            client.V1EnvVar(
                name="START_TIMESTAMP",
                value=datetime_formatter(mini_batch[0]),
            ),
            client.V1EnvVar(
                name="END_TIMESTAMP",
                value=datetime_formatter(mini_batch[1]),
            ),
            client.V1EnvVar(
                name="NO_CHECKS",
                value=os.environ.get("NO_CHECKS"),
            ),
            client.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value=os.environ.get("AWS_ACCESS_KEY_ID"),
            ),
            client.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            ),
        ]

        # Entrypoint configmap name
        entrypoint_configmap_name = f"delegation-verify-coordinator-worker"

        # Define the volumes
        auth_volume = client.V1Volume(
            name="auth-volume",
            empty_dir=client.V1EmptyDirVolumeSource(),
        )

        entrypoint_volume = client.V1Volume(
            name="entrypoint-volume",
            config_map=client.V1ConfigMapVolumeSource(
                name=entrypoint_configmap_name, default_mode=int("0777", 8)
            ),  # 0777 permission in octal as int
        )

        cassandra_ssl_volume = client.V1Volume(
            name="cassandra-crt",
            secret=client.V1SecretVolumeSource(
                secret_name="uptime-service-cassandra-crt"
            ),
        )

        # Define the volumeMounts
        auth_volume_mount = client.V1VolumeMount(
            name="auth-volume",
            mount_path=os.environ.get("AUTH_VOLUME_MOUNT_PATH"),
        )

        cassandra_ssl_volume_mount = client.V1VolumeMount(
            name="cassandra-crt",
            mount_path="/certs",
        )

        entrypoint_volume_mount = client.V1VolumeMount(
            name="entrypoint-volume",
            mount_path="/bin/entrypoint",
        )

        # Define resources for app and init container
        resource_requirements_container = client.V1ResourceRequirements(
            limits={"cpu": worker_cpu_limit, "memory": worker_memory_limit},
            requests={"cpu": worker_cpu_request, "memory": worker_memory_request},
        )

        # Define the container
        container = client.V1Container(
            name="delegation-verify",
            image=f"{worker_image}:{worker_tag}",
            command=[
                "/bin/entrypoint/entrypoint-worker.sh"
            ],  # The entrypoint script is in the cluster as a configmap. The script can be found in the helm chart of coordinator
            resources=resource_requirements_container,
            env=env_vars,
            image_pull_policy=os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent"),
            volume_mounts=[auth_volume_mount, entrypoint_volume_mount, cassandra_ssl_volume_mount],
        )

        # Define the init container
        init_container = client.V1Container(
            name="delegation-verify-init",
            image=f"{worker_image}:{worker_tag}",
            # command=["/bin/authenticate.sh"],
            command=["ls"],
            env=env_vars,
            image_pull_policy=os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent"),
            volume_mounts=[auth_volume_mount],
        )

        # Create the job
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                ttl_seconds_after_finished=ttl_seconds,
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        init_containers=[init_container],
                        containers=[container],
                        restart_policy="Never",
                        service_account_name=service_account_name,
                        volumes=[auth_volume, entrypoint_volume, cassandra_ssl_volume],
                    )
                ),
            ),
        )

        # Create the job and configmap in Kubernetes
        try:
            api_batch.create_namespaced_job(namespace, job)
            logging.info(
                f"Job {job_name} created in namespace {namespace}; start: {datetime_formatter(mini_batch[0])}, end: {datetime_formatter(mini_batch[1])}."
            )
            jobs.append(job_name)
        except Exception as e:
            logging.error(f"Error creating job {job_name}: {e}")

    # Monitor jobs
    while jobs:
        for job_name in list(jobs):
            try:
                job_status = api_batch.read_namespaced_job_status(job_name, namespace)
                if job_status.status.succeeded:
                    logging.info(f"Job {job_name} succeeded.")
                    jobs.remove(job_name)
                elif job_status.status.failed:
                    logging.error(f"Job {job_name} failed.")
                    jobs.remove(job_name)
            except Exception as e:
                logging.error(f"Error reading job status for {job_name}: {e}")

        time.sleep(10)

    logging.info("All jobs have been processed.")


def setUpValidatorProcesses(time_intervals, logging, worker_image, worker_tag):
    processes = []
    if bool_env_var_set("NO_CHECKS"):
        logging.info("stateless-verifier will run with --no-checks flag")
    for index, mini_batch in enumerate(time_intervals):
        process_name = (
            f"local-validator-{datetime.now().strftime('%y-%m-%d-%H-%M')}-{index}"
        )
        image = f"{worker_image}:{worker_tag}"
        cassandra_ip = try_get_hostname_ip(os.environ.get("CASSANDRA_HOST"), logging)
        command = [
            "docker",
            "run",
            # "--privileged",
            # "--network",
            # "host",
            "--rm",
            "-v",
            f"{os.environ.get('SSL_CERTFILE')}:/var/ssl/ssl-cert.crt",
            "-e",
            f"CASSANDRA_HOST={cassandra_ip}",
            "-e",
            "CASSANDRA_PORT",
            "-e",
            "CASSANDRA_USERNAME",
            "-e",
            "CASSANDRA_PASSWORD",
            "-e",
            "AWS_ACCESS_KEY_ID",
            "-e",
            "AWS_SECRET_ACCESS_KEY",
            "-e",
            "AWS_DEFAULT_REGION",
            "-e",
            "AWS_S3_BUCKET",
            "-e",
            "NETWORK_NAME",
            "-e",
            "SSL_CERTFILE=/var/ssl/ssl-cert.crt",
            "-e",
            "CASSANDRA_USE_SSL=1",
            "-e",
            "CQLSH=/bin/cqlsh-expansion",
            image,
            "cassandra",
            "--keyspace",
            os.environ.get("AWS_KEYSPACE"),
            f"{datetime_formatter(mini_batch[0])}",
            f"{datetime_formatter(mini_batch[1])}",
        ]
        if bool_env_var_set("NO_CHECKS"):
            command.append("--no-checks")
        cmd_str = " ".join(command)

        # Set up environment variables for the process
        env = os.environ.copy()
        env["JobName"] = process_name
        env["BatchStart"] = str(mini_batch[0])
        env["BatchEnd"] = str(mini_batch[1])

        # Spawn the process
        proc = subprocess.Popen(
            command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        processes.append((process_name, proc))
        logging.info(f"Launching process {index}: {cmd_str}")

    # Monitor the processes
    while processes:
        for process_name, proc in processes:
            if proc.poll() is not None:  # Check if the process has completed
                # Handle process stdout and stderr and remove it
                output, errors = proc.communicate()
                logging.info(f"Process {process_name} completed at {datetime.now()}")
                if output:
                    logging.info(f"{process_name} (stdout): {output.decode().strip()}")
                if errors:
                    logging.error(f"{process_name} (stderr): {errors.decode().strip()}")

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
        worker_image=os.environ.get("WORKER_IMAGE"),
        worker_tag=os.environ.get("WORKER_TAG"),
    )
