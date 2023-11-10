from kubernetes import client
import os
from datetime import datetime

# Set environment variables
# Get the values of your-image and tag from environment variables
worker_image = os.environ.get("WORKER_IMAGE", "busybox")
worker_tag = os.environ.get("WORKER_TAG", "latest")

def setUpValidatorPods(time_intervals, jobs, logging, worker_image, worker_tag):
    # Create a Kubernetes API client
    api = client.BatchV1Api()
    for index, mini_batch in enumerate(time_intervals):
                    job_name = f"zk-validator-{datetime.now().strftime('%y-%m-%d-%H-%M')}-{index}" # ZKValidator
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
                                                client.V1EnvVar(name="JobName", value=f"{job_name}"),
                                                client.V1EnvVar(name="BatchStart", value=f'{mini_batch[index][0]}'),
                                                client.V1EnvVar(name="BatchEnd", value=f'{mini_batch[index][1]}')
                                            ],
                                            image_pull_policy="Always",  # Set the image pull policy here
                                        )
                                    ],
                                    restart_policy="Never",
                                )
                            )
                        )
                    )
                    namespace = open("/var/run/secrets/kubernetes.io/serviceaccount/namespace").read()
                    api.create_namespaced_job(namespace, job)

                    logging.info(f"Launching pod {index}")
                              
    while True:
        for job_name in jobs:
            pod = api.read_namespaced_job(job_name, namespace)
            if pod.status.succeeded is not None and pod.status.succeeded > 0:
                logging.info(f"Pod {index} completed at {datetime.now()}")
                jobs.remove(job_name)
        if len(jobs)==0:
            break