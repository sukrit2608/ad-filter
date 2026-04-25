from fastapi import FastAPI
import databases
import sqlalchemy

DATABASE_URL = "sqlite:///./installed_apps.db"

database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

installed_apps_table = sqlalchemy.Table(
    "installed_apps",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("user_id", sqlalchemy.String),
    sqlalchemy.Column("app_package", sqlalchemy.String),
)

filter_log_table = sqlalchemy.Table(
    "filter_log",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("user_id", sqlalchemy.String),
    sqlalchemy.Column("app_package", sqlalchemy.String),
    sqlalchemy.Column("show_ad", sqlalchemy.Boolean),
    sqlalchemy.Column("split_ratio", sqlalchemy.Integer),
    sqlalchemy.Column("user_has_app", sqlalchemy.Boolean),
    sqlalchemy.Column("timestamp", sqlalchemy.String),
)

engine = sqlalchemy.create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
metadata.create_all(engine)

app = FastAPI()

@app.on_event("startup")
async def startup():
    await database.connect()
    count = await database.fetch_val("SELECT COUNT(*) FROM installed_apps")
    if count == 0:
        await database.execute_many(
            query=installed_apps_table.insert(),
            values=[
                {"user_id": "user_123", "app_package": "com.linkedin.android"},
                {"user_id": "user_123", "app_package": "com.google.android.apps.bard"},
                {"user_id": "user_123", "app_package": "com.instagram.android"},
                {"user_id": "user_456", "app_package": "com.spotify.music"},
                {"user_id": "user_456", "app_package": "com.whatsapp"},
            ]
        )

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.post("/filter")
async def filter_ad(user_id: str, app_package: str, split_ratio: int = 0):
    from datetime import datetime
    import random

    query = installed_apps_table.select().where(
        (installed_apps_table.c.user_id == user_id) &
        (installed_apps_table.c.app_package == app_package)
    )
    result = await database.fetch_one(query)
    user_has_app = result is not None

    if user_has_app:
        random_number = random.randint(0, 100)
        show_ad = random_number <= split_ratio
    else:
        show_ad = True

    await database.execute(
        filter_log_table.insert().values(
            user_id=user_id,
            app_package=app_package,
            show_ad=show_ad,
            split_ratio=split_ratio,
            user_has_app=user_has_app,
            timestamp=datetime.utcnow().isoformat()
        )
    )

    if show_ad:
        return {"show_ad": True, "reason": "app not installed, show the ad" if not user_has_app else "existing user within split ratio, show reminder ad"}
    else:
        return {"show_ad": False, "reason": "app already installed, outside split ratio"}

@app.post("/add-app")
async def add_app(user_id: str, app_package: str):
    query = installed_apps_table.insert().values(
        user_id=user_id,
        app_package=app_package
    )
    await database.execute(query)
    return {"message": f"Added {app_package} for {user_id}"}

@app.get("/user-apps")
async def get_user_apps(user_id: str):
    query = installed_apps_table.select().where(
        installed_apps_table.c.user_id == user_id
    )
    results = await database.fetch_all(query)
    apps = [row["app_package"] for row in results]
    return {"user_id": user_id, "installed_apps": apps}

@app.get("/analytics")
async def get_analytics():
    total_filtered = await database.fetch_val(
        "SELECT COUNT(*) FROM filter_log WHERE show_ad = 0"
    )
    total_shown = await database.fetch_val(
        "SELECT COUNT(*) FROM filter_log WHERE show_ad = 1"
    )
    reminder_ads = await database.fetch_val(
        "SELECT COUNT(*) FROM filter_log WHERE show_ad = 1 AND user_has_app = 1"
    )
    new_user_ads = await database.fetch_val(
        "SELECT COUNT(*) FROM filter_log WHERE show_ad = 1 AND user_has_app = 0"
    )
    top_apps = await database.fetch_all(
        """SELECT app_package, COUNT(*) as count 
           FROM filter_log WHERE show_ad = 0 
           GROUP BY app_package 
           ORDER BY count DESC LIMIT 5"""
    )
    avg_split_ratio = await database.fetch_val(
        "SELECT AVG(split_ratio) FROM filter_log WHERE user_has_app = 1"
    )
    return {
        "total_ads_filtered": total_filtered,
        "total_ads_shown": total_shown,
        "breakdown": {
            "reminder_ads_to_existing_users": reminder_ads,
            "new_user_acquisition_ads": new_user_ads,
        },
        "average_split_ratio_for_existing_users": round(avg_split_ratio or 0, 1),
        "top_filtered_apps": [{"app": row["app_package"], "count": row["count"]} for row in top_apps]
    }
from fastapi.responses import FileResponse
@app.get("/")
def read_root():
    return FileResponse("index.html")