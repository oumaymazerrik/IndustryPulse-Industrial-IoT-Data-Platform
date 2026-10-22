
import os
import joblib
import pandas as pd

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

# ==========================================
# 1. CONFIGURATION
# ==========================================

MODEL_PATH = "/app/models/xgboost_recall_priority.pkl"

app = FastAPI(
    title="IndustryPulse Predictive Maintenance API",
    description="Machine failure prediction using XGBoost",
    version="1.0.0"
)

# ==========================================
# 2. LOAD TRAINED MODEL
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
# 3. INPUT DATA SCHEMA
# ==========================================

class MachineData(BaseModel):
    air_temperature: float = Field(..., description="Air temperature in Kelvin")
    process_temperature: float = Field(..., description="Process temperature in Kelvin")
    rotational_speed: float = Field(..., description="Rotational speed in RPM")
    torque: float = Field(..., description="Torque in Nm")
    tool_wear: float = Field(..., description="Tool wear in minutes")


# ==========================================
# 4. HEALTH ENDPOINT
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
# 5. PREDICTION ENDPOINT
# ==========================================

@app.post("/predict")
def predict(data: MachineData):

    if model is None:
        raise HTTPException(
            status_code=503,
            detail="XGBoost model is not available"
        )

    input_data = pd.DataFrame(
        [data.model_dump()]
    )

    input_data = input_data[features]

    probability = float(
        model.predict_proba(input_data)[0][1]
    )

    prediction = int(probability >= threshold)

    return {
        "model": "XGBoost",
        "failure_probability": round(probability, 4),
        "threshold": threshold,
        "prediction": prediction,
        "status": "FAILURE_RISK" if prediction == 1 else "NORMAL"
    }
