import apscheduler.job
import apscheduler.schedulers.background
import apscheduler.jobstores.mongodb
import apscheduler.jobstores.memory
import apscheduler.executors.pool
import apscheduler.triggers.cron
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

import configparser
import datetime
import logging
import uuid
import os

logging.basicConfig(level=logging.INFO)
config = configparser.ConfigParser()
config.read("config.ini")
scheduler_config = config["SCHEDULER"]
mongodb_config = config["MONGODB"]
mongodb_uri = mongodb_config.get("URI").format_map(
    {
        "mongodb_username": os.getenv("MONGODB_USERNAME"),
        "mongodb_password": os.getenv("MONGODB_PASSWORD"),
        "mongodb_cluster_name": os.getenv("MONGODB_CLUSTER_NAME"),
    }
)
client = MongoClient(mongodb_uri, server_api=ServerApi("1"))
try:
    client.admin.command("ping")
except Exception as e:
    logging.error("Could not connect to MongoDB")
    logging.exception(e)
job_logs_collection = client.get_database(
    name=mongodb_config.get("DATABASE")
).get_collection(name=mongodb_config.get("JOB_LOGS_COLLECTION"))
job_stores = {
    "default": apscheduler.jobstores.mongodb.MongoDBJobStore(
        database=mongodb_config.get("DATABASE"),
        collection=mongodb_config.get("JOBS_COLLECTION"),
        client=client,
    ),
}
executors = {
    "default": apscheduler.executors.pool.ThreadPoolExecutor(
        max_workers=scheduler_config.getint("NUM_EXECUTORS"),
    ),
}
job_defaults = {
    "coalesce": scheduler_config.getboolean("COALESCE_JOBS"),
    "max_instances": scheduler_config.getint("MAX_JOB_INSTANCES"),
}
scheduler = apscheduler.schedulers.background.BackgroundScheduler(
    jobstores=job_stores,
    executors=executors,
    job_defaults=job_defaults,
)
scheduler.start()


def make_web_request(url: str, method: str, payload: dict, job_id: str):
    start_time = datetime.datetime.now(tz=datetime.timezone.utc)
    response = requests.request(
        url=url,
        method=method,
        json=payload,
    )
    end_time = datetime.datetime.now(tz=datetime.timezone.utc)
    elapsed_time = end_time - start_time
    response_status = response.status_code
    job_logs_collection.insert_one(
        {
            "type": "EXECUTION",
            "job_id": job_id,
            "time": start_time.timestamp(),
            "elapsed_time_ms": elapsed_time.total_seconds() * 1000,
            "response_status": response_status,
        }
    )


def schedule_job(
    url: str, method: str, frequency: str, payload: dict
) -> dict[str, str]:
    job_id = str(uuid.uuid4())
    scheduler.add_job(
        func=make_web_request,
        kwargs={
            "url": url,
            "method": method,
            "payload": payload,
            "job_id": job_id,
        },
        trigger=apscheduler.triggers.cron.CronTrigger.from_crontab(expr=frequency),
        id=job_id,
    )
    event_time = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    job_logs_collection.insert_one(
        {
            "type": "CREATION",
            "job_id": job_id,
            "time": event_time,
        }
    )
    return {"job_id": job_id}


def delete_job(job_id: str):
    job: apscheduler.job.Job = scheduler.get_job(job_id)
    if not job:
        raise ValueError("Job not found")
    job.remove()
    event_time = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    job_logs_collection.insert_one(
        {
            "type": "DELETION",
            "job_id": job_id,
            "time": event_time,
        }
    )


def get_scheduled_jobs():
    jobs: list[apscheduler.job.Job] = scheduler.get_jobs()
    job_details = []
    for job in jobs:
        args = job.kwargs
        args.pop("job_id")
        job_details.append(
            {
                "id": job.id,
                "args": args,
                "schedule": str(job.trigger),
                "next_run_time": job.next_run_time.isoformat(),
            }
        )
    return [
        {
            "id": job.id,
            "args": job.kwargs,
            "schedule": str(job.trigger),
            "next_run_time": job.next_run_time.isoformat(),
        }
        for job in jobs
    ]


def get_job_details(job_id: str, limit: int = 10):
    job: apscheduler.job.Job = scheduler.get_job(job_id)
    if not job:
        raise ValueError("Job not found")
    job_logs = list(
        client.get_database("apscheduler")
        .get_collection("job_logs")
        .find({"job_id": job_id}, limit=limit, sort=[("start_time", 1)])
    )
    args = job.kwargs
    args.pop("job_id")
    return {
        "id": job.id,
        "args": args,
        "schedule": str(job.trigger),
        "next_run_time": job.next_run_time.isoformat(),
        "job_logs": [
            {
                "start_time": log["start_time"].isoformat(),
                "end_time": log["end_time"].isoformat(),
                "response_status": log["response_status"],
            }
            for log in job_logs
        ],
    }
