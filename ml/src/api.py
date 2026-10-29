
import os
import time
import joblib
import pandas as pd

from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ==========================================
# 1. CONFIGURATION
# ==========================================

MODEL_PATH = os.getenv(
    "MODEL_PATH",
    "/app/models/xgboost_recall_priority.pkl"
)

app = FastAPI(
    title="IndustryPulse Predictive Maintenance API",
    description="Machine failure prediction using XGBoost",
    version="1.1.0"
)

# ==========================================
# 2. PROMETHEUS METRICS
# ==========================================

PREDICTIONS_TOTAL = Counter(
    "industrypulse_predictions_total",
    "Total number of predictions",
    ["status"]
)

PREDICTION_ERRORS = Counter(
    "industrypulse_prediction_errors_total",
    "Total number of prediction errors"
)

PREDICTION_DURATION = Histogram(
    "industrypulse_prediction_duration_seconds",
    "Prediction processing duration in seconds"
)

# ==========================================
# 3. LOAD TRAINED MODEL
# ==========================================

model = None
threshold = None
features = None

if os.path.exists(MODEL_PATH):
    saved = joblib.load(MODEL_PATH)

    model = saved["model"]
    threshold = saved["threshold"]
    features = saved["features"]

# ==========================================
# 4. INPUT DATA SCHEMA
# ==========================================

class MachineData(BaseModel):
    air_temperature: float = Field(
        ..., description="Air temperature in Kelvin"
    )
    process_temperature: float = Field(
        ..., description="Process temperature in Kelvin"
    )
    rotational_speed: float = Field(
        ..., description="Rotational speed in RPM"
    )
    torque: float = Field(
        ..., description="Torque in Nm"
    )
    tool_wear: float = Field(
        ..., description="Tool wear in minutes"
    )

# ==========================================
# 5. HEALTH ENDPOINT
# ==========================================

@app.get("/health")
def health():
    return {
        "status": "ok" if model is not None else "model_unavailable",
        "model": "XGBoost",
        "model_loaded": model is not None,
        "threshold": threshold
    }

# ==========================================
# 6. METRICS ENDPOINT
# ==========================================

@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

# ==========================================
# 7. PREDICTION ENDPOINT
# ==========================================

@app.post("/predict")
def predict(data: MachineData):

    start_time = time.perf_counter()

    try:
        if model is None:
            raise HTTPException(
                status_code=503,
                detail="XGBoost model is not available"
            )

        input_data = pd.DataFrame([data.model_dump()])
        input_data = input_data[features]

        probability = float(
            model.predict_proba(input_data)[0][1]
        )

        prediction = int(probability >= threshold)

        status = (
            "FAILURE_RISK" if prediction == 1 else "NORMAL"
        )

        PREDICTIONS_TOTAL.labels(status=status).inc()

        return {
            "model": "XGBoost",
            "failure_probability": round(probability, 4),
            "threshold": threshold,
            "prediction": prediction,
            "status": status
        }

    except HTTPException:
        PREDICTION_ERRORS.inc()
        raise

    except Exception as exc:
        PREDICTION_ERRORS.inc()
        raise HTTPException(
            status_code=500,
            detail="Prediction processing failed"
        ) from exc

    finally:
        PREDICTION_DURATION.observe(
            time.perf_counter() - start_time
        )
