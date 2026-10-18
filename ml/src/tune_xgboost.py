
import os
import json
import joblib
import pandas as pd
import mlflow
import mlflow.sklearn

from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
    classification_report
)


# ==========================================
# 1. CONFIGURATION
# ==========================================

DATA_PATH = "/app/data/ai4i2020.csv"
MODEL_DIR = "/app/models"
REPORT_DIR = "/app/reports"

TARGET_RECALL = 0.95
MIN_PRECISION = 0.30

os.makedirs(MODEL_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

mlflow.set_tracking_uri("file:///app/mlruns")
mlflow.set_experiment("IndustryPulse-Predictive-Maintenance")


# ==========================================
# 2. LOAD DATASET
# ==========================================

df = pd.read_csv(DATA_PATH)

feature_mapping = {
    "Air temperature [K]": "air_temperature",
    "Process temperature [K]": "process_temperature",
    "Rotational speed [rpm]": "rotational_speed",
    "Torque [Nm]": "torque",
    "Tool wear [min]": "tool_wear"
}

X = df[list(feature_mapping.keys())].copy()
X = X.rename(columns=feature_mapping)

y = df["Machine failure"]

features = X.columns.tolist()

print("Dataset shape:", df.shape)

print("\nClass distribution:")
print(y.value_counts())


# ==========================================
# 3. TRAIN / VALIDATION / TEST SPLIT
# ==========================================

X_train_val, X_test, y_train_val, y_test = train_test_split(
    X,
    y,
    test_size=0.20,
    random_state=42,
    stratify=y
)

X_train, X_val, y_train, y_val = train_test_split(
    X_train_val,
    y_train_val,
    test_size=0.25,
    random_state=42,
    stratify=y_train_val
)

print("\nTraining samples:", len(X_train))
print("Validation samples:", len(X_val))
print("Test samples:", len(X_test))


# ==========================================
# 4. TRAIN XGBOOST
# ==========================================

negative_count = (y_train == 0).sum()
positive_count = (y_train == 1).sum()

imbalance_ratio = negative_count / positive_count

model = XGBClassifier(
    n_estimators=200,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    scale_pos_weight=imbalance_ratio,
    eval_metric="logloss",
    random_state=42,
    n_jobs=2
)

print("\nTraining XGBoost...")

model.fit(X_train, y_train)

print("Training completed!")


# ==========================================
# 5. THRESHOLD SEARCH ON VALIDATION
# ==========================================

val_probabilities = model.predict_proba(X_val)[:, 1]

results = []

for i in range(1, 100):

    threshold = i / 100

    predictions = (
        val_probabilities >= threshold
    ).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_val,
        predictions,
        labels=[0, 1]
    ).ravel()

    precision = precision_score(
        y_val,
        predictions,
        zero_division=0
    )

    recall = recall_score(
        y_val,
        predictions,
        zero_division=0
    )

    f1 = f1_score(
        y_val,
        predictions,
        zero_division=0
    )

    results.append({
        "threshold": threshold,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "TN": int(tn),
        "FP": int(fp),
        "FN": int(fn),
        "TP": int(tp)
    })

results_df = pd.DataFrame(results)

threshold_report = os.path.join(
    REPORT_DIR,
    "xgboost_threshold_search_recall.csv"
)

results_df.to_csv(
    threshold_report,
    index=False
)


# ==========================================
# 6. RECALL-ORIENTED THRESHOLD SELECTION
# ==========================================

eligible = results_df[
    results_df["precision"] >= MIN_PRECISION
].copy()

if eligible.empty:

    print("\nNo threshold satisfies minimum precision.")
    print("Selecting highest recall as fallback.")

    eligible = results_df.copy()
    selection_rule = "fallback_max_recall"

else:

    selection_rule = "maximize_recall_with_min_precision"

# Highest recall first, lowest false positives second
selected = eligible.sort_values(
    ["recall", "FP"],
    ascending=[False, True]
).iloc[0]

best_threshold = float(selected["threshold"])

print("\n" + "=" * 60)
print("VALIDATION THRESHOLD SELECTION")
print("=" * 60)

print("Selection rule:", selection_rule)
print("Selected threshold:", best_threshold)
print("Target recall:", TARGET_RECALL)
print("Validation precision:", round(selected["precision"], 4))
print("Validation recall:", round(selected["recall"], 4))
print("Validation F1:", round(selected["f1_score"], 4))
print("Validation FP:", int(selected["FP"]))
print("Validation FN:", int(selected["FN"]))

if selected["recall"] >= TARGET_RECALL:
    print("Target recall achieved on validation!")
else:
    print("Target recall NOT achieved on validation.")


# ==========================================
# 7. TEST EVALUATION
# ==========================================

test_probabilities = model.predict_proba(X_test)[:, 1]

test_predictions = (
    test_probabilities >= best_threshold
).astype(int)

tn, fp, fn, tp = confusion_matrix(
    y_test,
    test_predictions,
    labels=[0, 1]
).ravel()

test_metrics = {
    "threshold": best_threshold,
    "accuracy": accuracy_score(
        y_test,
        test_predictions
    ),
    "precision": precision_score(
        y_test,
        test_predictions,
        zero_division=0
    ),
    "recall": recall_score(
        y_test,
        test_predictions,
        zero_division=0
    ),
    "f1_score": f1_score(
        y_test,
        test_predictions,
        zero_division=0
    ),
    "TN": int(tn),
    "FP": int(fp),
    "FN": int(fn),
    "TP": int(tp)
}

print("\n" + "=" * 60)
print("XGBOOST TEST RESULTS")
print("=" * 60)

for name, value in test_metrics.items():
    if isinstance(value, float):
        print(f"{name}: {value:.4f}")
    else:
        print(f"{name}: {value}")

print("\nConfusion Matrix:")
print([[int(tn), int(fp)], [int(fn), int(tp)]])

print("\nClassification Report:")
print(
    classification_report(
        y_test,
        test_predictions,
        zero_division=0
    )
)


# ==========================================
# 8. SAVE MODEL
# ==========================================

model_path = os.path.join(
    MODEL_DIR,
    "xgboost_recall_priority.pkl"
)

joblib.dump(
    {
        "model": model,
        "threshold": best_threshold,
        "features": features,
        "feature_mapping": feature_mapping,
        "selection_rule": selection_rule,
        "target_recall": TARGET_RECALL,
        "min_precision": MIN_PRECISION
    },
    model_path
)


# ==========================================
# 9. SAVE REPORT
# ==========================================

report_path = os.path.join(
    REPORT_DIR,
    "xgboost_recall_priority.json"
)

validation_metrics = {
    "threshold": best_threshold,
    "precision": float(selected["precision"]),
    "recall": float(selected["recall"]),
    "f1_score": float(selected["f1_score"]),
    "TN": int(selected["TN"]),
    "FP": int(selected["FP"]),
    "FN": int(selected["FN"]),
    "TP": int(selected["TP"])
}

with open(report_path, "w") as file:

    json.dump(
        {
            "selection_rule": selection_rule,
            "target_recall": TARGET_RECALL,
            "min_precision": MIN_PRECISION,
            "validation_metrics": validation_metrics,
            "test_metrics": test_metrics
        },
        file,
        indent=4
    )


# ==========================================
# 10. MLFLOW TRACKING
# ==========================================

with mlflow.start_run(
    run_name="XGBoost-Recall-Priority"
):

    mlflow.log_param("model", "XGBoost")
    mlflow.log_param("target_recall", TARGET_RECALL)
    mlflow.log_param("min_precision", MIN_PRECISION)
    mlflow.log_param("threshold", best_threshold)
    mlflow.log_param("selection_rule", selection_rule)
    mlflow.log_param("scale_pos_weight", imbalance_ratio)

    for name, value in validation_metrics.items():
        if name != "threshold":
            mlflow.log_metric(
                "validation_" + name,
                float(value)
            )

    for name, value in test_metrics.items():
        if name != "threshold":
            mlflow.log_metric(
                "test_" + name,
                float(value)
            )

    mlflow.log_artifact(report_path)
    mlflow.log_artifact(threshold_report)

    mlflow.sklearn.log_model(
        model,
        artifact_path="model"
    )


# ==========================================
# 11. FINAL SUMMARY
# ==========================================

print("\n" + "=" * 60)
print("FINAL SUMMARY")
print("=" * 60)

print("Model: XGBoost")
print("Selected threshold:", best_threshold)
print("Selection rule:", selection_rule)

print("\nValidation:")
print("Recall:", round(validation_metrics["recall"], 4))
print("FN:", validation_metrics["FN"])
print("FP:", validation_metrics["FP"])

print("\nTest:")
print("Recall:", round(test_metrics["recall"], 4))
print("FN:", test_metrics["FN"])
print("FP:", test_metrics["FP"])

print("\nModel saved:", model_path)
print("Report saved:", report_path)

print("\nRecall optimization completed!")
