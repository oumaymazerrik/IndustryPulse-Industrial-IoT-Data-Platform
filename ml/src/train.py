
import os
import json
import joblib
import pandas as pd
import mlflow
import mlflow.sklearn

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    classification_report,
    confusion_matrix
)

# 1. Load dataset
df = pd.read_csv("/app/data/ai4i2020.csv")

print("Dataset shape:", df.shape)
print("Failure distribution:")
print(df["Machine failure"].value_counts())

# 2. Select features and target
features = [
    "Air temperature [K]",
    "Process temperature [K]",
    "Rotational speed [rpm]",
    "Torque [Nm]",
    "Tool wear [min]"
]

X = df[features]
y = df["Machine failure"]

# 3. Train / Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y
)

# 4. MLflow configuration
mlflow.set_tracking_uri("file:///app/mlruns")
mlflow.set_experiment("IndustryPulse-Predictive-Maintenance")

os.makedirs("/app/models", exist_ok=True)
os.makedirs("/app/reports", exist_ok=True)

with mlflow.start_run(run_name="RandomForest-Baseline"):

    # 5. Model training
    model = RandomForestClassifier(
        n_estimators=100,
        random_state=42,
        class_weight="balanced"
    )

    model.fit(X_train, y_train)

    # 6. Prediction
    y_pred = model.predict(X_test)

    # 7. Evaluation
    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "precision": precision_score(
            y_test, y_pred, zero_division=0
        ),
        "recall": recall_score(
            y_test, y_pred, zero_division=0
        ),
        "f1_score": f1_score(
            y_test, y_pred, zero_division=0
        )
    }

    print("\nModel Performance:")
    for name, value in metrics.items():
        print(f"{name}: {value:.4f}")

    print("\nClassification Report:")
    print(classification_report(
        y_test, y_pred, zero_division=0
    ))

    print("\nConfusion Matrix:")
    print(confusion_matrix(y_test, y_pred))

    # 8. MLflow tracking
    mlflow.log_param("model", "RandomForestClassifier")
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("class_weight", "balanced")
    mlflow.log_param("test_size", 0.2)

    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(model, "model")

    # 9. Save model and metrics
    joblib.dump(model, "/app/models/random_forest.pkl")

    with open("/app/reports/metrics.json", "w") as file:
        json.dump(metrics, file, indent=4)

print("\nTraining completed successfully!")
