import fastapi

import app.handlers

job_router = fastapi.APIRouter()


@job_router.post("/jobs")
def schedule_job(url: str, method: str, frequency: str, payload: dict):
    return app.handlers.schedule_job(
        url=url,
        method=method,
        frequency=frequency,
        payload=payload,
    )


@job_router.get("/jobs/{job_id}")
def get_job_details(job_id: str):
    try:
        return app.handlers.get_job_details(job_id=job_id, limit=10)
    except ValueError:
        return fastapi.HTTPException(status_code=404, detail="Job not found")


@job_router.get("/jobs")
def get_scheduled_jobs():
    return app.handlers.get_scheduled_jobs()


@job_router.delete("/jobs/{job_id}")
def delete_job(job_id: str):
    try:
        return app.handlers.delete_job(job_id)
    except ValueError:
        return fastapi.HTTPException(status_code=404, detail="Job not found")
