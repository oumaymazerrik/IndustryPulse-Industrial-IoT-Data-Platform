
import os
import joblib
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix
)

# ==========================================
# 1. CONFIGURATION
# ==========================================

DATA_PATH = "/app/data/ai4i2020.csv"
MODEL_PATH = "/app/models/xgboost_recall_priority.pkl"
REPORT_DIR = "/app/reports"

os.makedirs(REPORT_DIR, exist_ok=True)

thresholds = [
    0.10, 0.20, 0.25, 0.30,
    0.33, 0.35, 0.40, 0.45,
    0.50, 0.60, 0.85
]

# ==========================================
# 2. LOAD MODEL
# ==========================================

saved = joblib.load(MODEL_PATH)

model = saved["model"]
features = saved["features"]
feature_mapping = saved["feature_mapping"]

print("XGBoost model loaded successfully!")

# ==========================================
# 3. LOAD DATA
# ==========================================

df = pd.read_csv(DATA_PATH)

X = df[list(feature_mapping.keys())].copy()
X = X.rename(columns=feature_mapping)

X = X[features]

y = df["Machine failure"]

# Reproduce the same train/validation split
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

# ==========================================
# 4. COMPARE ON VALIDATION DATA
# ==========================================

probabilities = model.predict_proba(X_val)[:, 1]

results = []

for threshold in thresholds:

    predictions = (
        probabilities >= threshold
    ).astype(int)

    tn, fp, fn, tp = confusion_matrix(
        y_val,
        predictions,
        labels=[0, 1]
    ).ravel()

    results.append({
        "threshold": threshold,
        "recall": round(
            recall_score(y_val, predictions, zero_division=0),
            4
        ),
        "precision": round(
            precision_score(y_val, predictions, zero_division=0),
            4
        ),
        "f1_score": round(
            f1_score(y_val, predictions, zero_division=0),
            4
        ),
        "accuracy": round(
            accuracy_score(y_val, predictions),
            4
        ),
        "TP": int(tp),
        "FN": int(fn),
        "FP": int(fp),
        "TN": int(tn)
    })

results_df = pd.DataFrame(results)

# ==========================================
# 5. DISPLAY RESULTS
# ==========================================

print("\n" + "=" * 80)
print("XGBOOST - VALIDATION THRESHOLD COMPARISON")
print("=" * 80)

print(results_df.to_string(index=False))

# ==========================================
# 6. SELECT BEST THRESHOLD
# ==========================================

MIN_PRECISION = 0.30

eligible = results_df[
    results_df["precision"] >= MIN_PRECISION
].copy()

if eligible.empty:
    print("\nNo threshold meets minimum precision.")
    selected = results_df.sort_values(
        ["recall", "FP"],
        ascending=[False, True]
    ).iloc[0]
else:
    selected = eligible.sort_values(
        ["recall", "FP"],
        ascending=[False, True]
    ).iloc[0]

print("\n" + "=" * 50)
print("RECOMMENDED THRESHOLD - VALIDATION")
print("=" * 50)

print("Threshold:", selected["threshold"])
print("Recall:", selected["recall"])
print("Precision:", selected["precision"])
print("FN:", int(selected["FN"]))
print("FP:", int(selected["FP"]))

# ==========================================
# 7. SAVE RESULTS
# ==========================================

report_path = os.path.join(
    REPORT_DIR,
    "xgboost_threshold_comparison.csv"
)

results_df.to_csv(report_path, index=False)

print("\nResults saved:", report_path)
print("Comparison completed!")
