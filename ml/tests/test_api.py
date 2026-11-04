
import sys
from pathlib import Path

from fastapi.testclient import TestClient

# Import API from ml/src
sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[1] / "src")
)

import api


client = TestClient(api.app)


def test_health_endpoint():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["model"] == "XGBoost"
    assert "model_loaded" in response.json()


def test_metrics_endpoint():
    response = client.get("/metrics")

    assert response.status_code == 200
    assert "industrypulse_prediction_errors_total" in response.text
    assert "industrypulse_validation_errors_total" in response.text


def test_invalid_prediction():
    payload = {
        "air_temperature": "INVALID",
        "process_temperature": 310,
        "rotational_speed": 1500,
        "torque": 40,
        "tool_wear": 100,
    }

    response = client.post("/predict", json=payload)

    assert response.status_code == 422


def test_missing_feature():
    payload = {
        "air_temperature": 300,
        "process_temperature": 310,
        "rotational_speed": 1500,
        "torque": 40,
    }

    response = client.post("/predict", json=payload)

    assert response.status_code == 422


def test_validation_error_metric():
    before = client.get("/metrics").text

    payload = {
        "air_temperature": "INVALID",
        "process_temperature": 310,
        "rotational_speed": 1500,
        "torque": 40,
        "tool_wear": 100,
    }

    response = client.post("/predict", json=payload)

    assert response.status_code == 422

    after = client.get("/metrics").text

    assert "industrypulse_validation_errors_total" in after
