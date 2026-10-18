
import os
import json
import joblib
import pandas as pd
import mlflow
import mlflow.sklearn

from imblearn.over_sampling import SMOTE
from xgboost import XGBClassifier
from catboost import CatBoostClassifier

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
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
# 3. TRAIN / VALIDATION / TEST
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
# 4. MODELS
# ==========================================

model_names = [
    "RandomForest-Baseline",
    "RandomForest-SMOTE",
    "XGBoost",
    "CatBoost"
]

thresholds = [
    round(i / 100, 2)
    for i in range(5, 96, 5)
]

all_results = []
validation_results = []


# ==========================================
# 5. TRAIN AND COMPARE
# ==========================================

for model_name in model_names:

    print("\n" + "=" * 60)
    print("Model:", model_name)
    print("=" * 60)

    X_train_used = X_train
    y_train_used = y_train

    negative_count = (y_train == 0).sum()
    positive_count = (y_train == 1).sum()

    imbalance_ratio = negative_count / positive_count

    # --------------------------------------
    # RANDOM FOREST + SMOTE
    # --------------------------------------

    if model_name == "RandomForest-SMOTE":

        smote = SMOTE(
            sampling_strategy=0.5,
            random_state=42
        )

        X_train_used, y_train_used = smote.fit_resample(
            X_train,
            y_train
        )

        model = RandomForestClassifier(
            n_estimators=100,
            random_state=42,
            n_jobs=2
        )

    # --------------------------------------
    # XGBOOST
    # --------------------------------------

    elif model_name == "XGBoost":

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

    # --------------------------------------
    # CATBOOST
    # --------------------------------------

    elif model_name == "CatBoost":

        model = CatBoostClassifier(
            iterations=500,
            depth=5,
            learning_rate=0.03,
            l2_leaf_reg=3,
            class_weights=[1, imbalance_ratio],
            loss_function="Logloss",
            random_seed=42,
            thread_count=2,
            verbose=False
        )

    # --------------------------------------
    # RANDOM FOREST BASELINE
    # --------------------------------------

    else:

        model = RandomForestClassifier(
            n_estimators=100,
            random_state=42,
            class_weight="balanced",
            n_jobs=2
        )

    print("\nTraining class distribution:")
    print(pd.Series(y_train_used).value_counts())

    # ======================================
    # 6. TRAIN MODEL
    # ======================================

    model.fit(X_train_used, y_train_used)

    # ======================================
    # 7. THRESHOLD TUNING
    # ======================================

    val_probabilities = model.predict_proba(X_val)[:, 1]

    best_threshold = 0.5
    best_f1 = -1

    print("\nValidation threshold comparison:")

    for threshold in thresholds:

        val_predictions = (
            val_probabilities >= threshold
        ).astype(int)

        precision = precision_score(
            y_val,
            val_predictions,
            zero_division=0
        )

        recall = recall_score(
            y_val,
            val_predictions,
            zero_division=0
        )

        f1 = f1_score(
            y_val,
            val_predictions,
            zero_division=0
        )

        tn_val, fp_val, fn_val, tp_val = confusion_matrix(
            y_val,
            val_predictions,
            labels=[0, 1]
        ).ravel()

        validation_results.append({
            "model": model_name,
            "threshold": threshold,
            "precision": precision,
            "recall": recall,
            "f1_score": f1,
            "FP": int(fp_val),
            "FN": int(fn_val)
        })

        print(
            f"Threshold: {threshold:.2f} | "
            f"Precision: {precision:.4f} | "
            f"Recall: {recall:.4f} | "
            f"F1: {f1:.4f} | "
            f"FN: {fn_val} | "
            f"FP: {fp_val}"
        )

        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold

    print("\nBest threshold:", best_threshold)
    print("Best validation F1:", round(best_f1, 4))

    # ======================================
    # 8. TEST EVALUATION
    # ======================================

    test_probabilities = model.predict_proba(X_test)[:, 1]

    test_predictions = (
        test_probabilities >= best_threshold
    ).astype(int)

    accuracy = accuracy_score(y_test, test_predictions)

    precision = precision_score(
        y_test, test_predictions, zero_division=0
    )

    recall = recall_score(
        y_test, test_predictions, zero_division=0
    )

    f1 = f1_score(
        y_test, test_predictions, zero_division=0
    )

    tn, fp, fn, tp = confusion_matrix(
        y_test,
        test_predictions,
        labels=[0, 1]
    ).ravel()

    print("\nTest Metrics:")
    print(f"Accuracy:  {accuracy:.4f}")
    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1-score:  {f1:.4f}")

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

    # ======================================
    # 9. SAVE MODEL
    # ======================================

    model_filenames = {
        "RandomForest-Baseline": "random_forest_baseline_comparison.pkl",
        "RandomForest-SMOTE": "random_forest_smote.pkl",
        "XGBoost": "xgboost_model.pkl",
        "CatBoost": "catboost_model.pkl"
    }

    model_path = os.path.join(
        MODEL_DIR,
        model_filenames[model_name]
    )

    joblib.dump(
        {
            "model": model,
            "threshold": best_threshold,
            "features": features,
            "feature_mapping": feature_mapping
        },
        model_path
    )

    # ======================================
    # 10. SAVE METRICS
    # ======================================

    result = {
        "model": model_name,
        "threshold": best_threshold,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "TN": int(tn),
        "FP": int(fp),
        "FN": int(fn),
        "TP": int(tp),
        "validation_f1": best_f1
    }

    all_results.append(result)

    # ======================================
    # 11. MLFLOW
    # ======================================

    with mlflow.start_run(
        run_name=model_name + "-Comparison"
    ):

        mlflow.log_param("model", model_name)
        mlflow.log_param("threshold", best_threshold)
        mlflow.log_param("imbalance_ratio", imbalance_ratio)
        mlflow.log_param(
            "smote",
            model_name == "RandomForest-SMOTE"
        )

        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("validation_f1", best_f1)

        mlflow.log_metric("TN", int(tn))
        mlflow.log_metric("FP", int(fp))
        mlflow.log_metric("FN", int(fn))
        mlflow.log_metric("TP", int(tp))

        mlflow.sklearn.log_model(
            model,
            artifact_path="model"
        )

    print("\nModel saved:", model_path)


# ==========================================
# 12. SAVE REPORTS
# ==========================================

comparison_df = pd.DataFrame(all_results)

comparison_df.to_csv(
    os.path.join(
        REPORT_DIR,
        "models_comparison.csv"
    ),
    index=False
)

pd.DataFrame(validation_results).to_csv(
    os.path.join(
        REPORT_DIR,
        "models_thresholds.csv"
    ),
    index=False
)

with open(
    os.path.join(
        REPORT_DIR,
        "models_comparison.json"
    ),
    "w"
) as file:

    json.dump(
        all_results,
        file,
        indent=4
    )


# ==========================================
# 13. FINAL COMPARISON
# ==========================================

print("\n" + "=" * 70)
print("FINAL COMPARISON")
print("=" * 70)

print(
    comparison_df[
        [
            "model",
            "threshold",
            "accuracy",
            "precision",
            "recall",
            "f1_score",
            "FP",
            "FN"
        ]
    ].to_string(index=False)
)

# Best model according to validation F1
best_model = comparison_df.loc[
    comparison_df["validation_f1"].idxmax()
]

print("\nBest model based on validation F1:")
print(best_model["model"])

print("\nAll experiments completed successfully!")
