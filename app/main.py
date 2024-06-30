import fastapi
import dotenv

from app.routers import job_router

dotenv.load_dotenv()
app = fastapi.FastAPI()
app.include_router(job_router)
