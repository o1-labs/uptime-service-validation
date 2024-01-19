import logging
import sys
from kubernetes import client, config
import os
from datetime import datetime, timezone
import subprocess
import time
import string
import random


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
    testnet = os.environ.get("TESTNET")

    service_account_name = f"{platform}-{testnet}-delegation-verify"

    worker_cpu_request = os.environ.get("WORKER_CPU_REQUEST")
    worker_memory_request = os.environ.get("WORKER_MEMORY_REQUEST")
    worker_cpu_limit = os.environ.get("WORKER_CPU_LIMIT")
    worker_memory_limit = os.environ.get("WORKER_MEMORY_LIMIT")

    # List to keep track of job names
    jobs = []

    # List to keep track of config maps
    configmaps = []

    for index, mini_batch in enumerate(time_intervals):
        # Define the environment variables
        env_vars = [
            client.V1EnvVar(name="CQLSH", value=os.environ.get("CQLSH")),
            client.V1EnvVar(
                name="AWS_ROLE_SESSION_NAME",
                value=os.environ.get("AWS_ROLE_SESSION_NAME").rstrip("-coordinator"),
            ),
            client.V1EnvVar(
                name="CASSANDRA_HOST", value=os.environ.get("CASSANDRA_HOST")
            ),
            client.V1EnvVar(
                name="CASSANDRA_PORT", value=os.environ.get("CASSANDRA_PORT")
            ),
            client.V1EnvVar(name="AWS_REGION", value=os.environ.get("AWS_REGION")),
            client.V1EnvVar(
                name="AWS_DEFAULT_REGION", value=os.environ.get("AWS_REGION")
            ),
            client.V1EnvVar(
                name="SSL_CERTFILE", value=os.environ.get("SSL_CERTFILE")
            ),
            client.V1EnvVar(name="CASSANDRA_USE_SSL", value="1"),
            client.V1EnvVar(
                name="AUTH_VOLUME_MOUNT_PATH",
                value=os.environ.get("AUTH_VOLUME_MOUNT_PATH"),
            ),
        ]

        # Variables and script for entrypoint configmap
        keyspace = os.environ.get("AWS_KEYSPACE")
        start_timestamp = datetime_formatter(mini_batch[0]).replace(" ", "\ ")
        end_timestamp = datetime_formatter(mini_batch[1]).replace(" ", "\ ")
        
        # Unique names are needed for the configmaps
        configmap_name_suffix = str(
            "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
        )

        # The entrypoint script does the following
        # Sources necessary credentials
        # Resolves the domain name of the database host with nslookup
        # Because for some unknown reason when resolving it throught the cqlsh binary the consistency of this working is lower than 50%
        # If nslookup failed retries and if it succeded to get an ip address executes the command

        script_content = f"""#!/bin/bash

# Source credentials for AWS
source /var/mina-delegation-verify-auth/.env

max_retries=5
retries=0
dns_name=$CASSANDRA_HOST
ip_address=""

while [ $retries -lt $max_retries ]; do
    # Perform nslookup and capture the output
    nslookup_result=$(nslookup $dns_name 2>/dev/null)

    # Check if nslookup was successful (exit code 0)
    if [ $? -eq 0 ]; then
        # Extract IP address from nslookup result
        ip_addresses=($(echo "$nslookup_result" | awk '/^Address: / {{print $2}}'))

        # Check if IP address is not empty
        if [ ${{#ip_addresses[@]}} -gt 0 ]; then
            break
        fi
    fi

    # Increment retry count
    ((retries++))

    # If not successful, wait for a moment before retrying
    sleep 2
done

# Use the result
if [ ${{#ip_addresses[@]}} -gt 0 ]; then
    CASSANDRA_HOST=${{ip_addresses[0]}}
    /bin/delegation-verify cassandra --keyspace {keyspace} {start_timestamp} {end_timestamp} --no-check
else
    echo "DNS resolution failed after $max_retries retries."
fi

"""

        # Entrypoint configmap name
        entrypoint_configmap_name = (
            f"delegation-verify-entrypoint-configmap-{configmap_name_suffix}"
        )

        # Entrypoint configmap
        entrypoint = client.V1ConfigMap(
            api_version="v1",
            kind="ConfigMap",
            metadata=client.V1ObjectMeta(name=entrypoint_configmap_name),
            data={"entrypoint.sh": script_content},
        )

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

        # Define the volumeMounts
        auth_volume_mount = client.V1VolumeMount(
            name="auth-volume",
            mount_path=os.environ.get("AUTH_VOLUME_MOUNT_PATH"),
        )

        entrypoint_volume_mount = client.V1VolumeMount(
            name="entrypoint-volume",
            mount_path="/bin/entrypoint",
        )

        # Define resources for app and init container
        resource_requirements_container = client.V1ResourceRequirements(
            limits={"cpu": worker_cpu_limit, "memory": worker_memory_limit},
            requests={"cpu": worker_cpu_request, "memory": worker_memory_request}
        )

        # Define the container
        container = client.V1Container(
            name="delegation-verify",
            image=f"{worker_image}:{worker_tag}",
            command=["/bin/entrypoint/entrypoint.sh"],
            resources=resource_requirements_container,
            env=env_vars,
            image_pull_policy=os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent"),
            volume_mounts=[auth_volume_mount, entrypoint_volume_mount],
        )

        # Define the init container
        init_container = client.V1Container(
            name="delegation-verify-init",
            image=f"{worker_image}:{worker_tag}",
            command=["/bin/authenticate.sh"],
            env=env_vars,
            image_pull_policy=os.environ.get("IMAGE_PULL_POLICY", "IfNotPresent"),
            volume_mounts=[auth_volume_mount],
        )

        # Job name
        job_name = f"delegation-verify-{datetime.now(timezone.utc).strftime('%y-%m-%d-%H-%M')}-{index}"

        # Create the job
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=client.V1ObjectMeta(name=job_name),
            spec=client.V1JobSpec(
                template=client.V1PodTemplateSpec(
                    spec=client.V1PodSpec(
                        init_containers=[init_container],
                        containers=[container],
                        restart_policy="Never",
                        service_account_name=service_account_name,
                        volumes=[auth_volume, entrypoint_volume],
                    )
                )
            ),
        )

        # Create the job and configmap in Kubernetes
        try:
            api_core.create_namespaced_config_map(namespace, entrypoint)
            logging.info(
                f"ConfigMap {entrypoint_configmap_name} created in namespace {namespace}"
            )
            configmaps.append(entrypoint_configmap_name)
            api_batch.create_namespaced_job(namespace, job)
            logging.info(f"Job {job_name} created in namespace {namespace}")
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
    
    while configmaps:
        for configmap_name in list(configmaps):
            try:
                api_core.delete_namespaced_config_map(configmap_name, namespace)
                configmaps.remove(configmap_name)
            except Exception as e:
                logging.error(f"Error deleting config map for {configmap_name}: {e}")

    logging.info("Configmaps have been deleted.")

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
            # "-it",
            "--privileged",
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
            "AWS_ACCESS_KEY_ID",
            "--env",
            "AWS_SECRET_ACCESS_KEY",
            "--env",
            "AWS_DEFAULT_REGION",
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
        #     "--no-check",
        # ]
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
