# ML5 — FastAPI Model Serving Test Report

## 1. Objective

ML5 operationalizes the trained ML3 risk classifier through the existing FastAPI prediction endpoint.

The objective is to verify that:

- The trained ML3 model is loaded successfully by the API.
- Incoming features follow the ML2 feature schema.
- The API performs real model inference.
- The API returns a risk score and risk level.
- The model version is exposed.
- Invalid feature values are rejected.
- The existing API contract remains unchanged.

---

## 2. Model Used

Model artifact:

`models/ml3_risk_classifier.joblib`

Model version:

`ml3-logistic-v1`

Model pipeline:

`StandardScaler → LogisticRegression`

Model classes:

`0, 1`

Class `1` represents elevated high-activity risk for the next hourly interval.

The ML3 target was defined using the training-period 90th percentile threshold:

`1177.6937`

The threshold is used to define the training target. ML5 uses the trained classifier probability for inference.

---

## 3. ML2 Feature Contract

The API accepts the six features used by the trained model:

1. `avg_activity`
2. `activity_growth`
3. `active_hours`
4. `peak_ratio`
5. `variability`
6. `internet_share`

Additional request fields:

- `grid_id`
- `feature_timestamp`

The API validates feature ranges using the existing Pydantic request contract.

---

## 4. Test 1 — Real Model Inference

### Input

Grid:

`147`

Feature timestamp:

`2013-11-06 23:00:00`

Features:

- `avg_activity = 1072.285421`
- `activity_growth = -0.132252`
- `active_hours = 24`
- `peak_ratio = 1.605623`
- `variability = 330.638381`
- `internet_share = 0.917552`

### Reference ML3 Result

From `ml3_predictions.csv`:

- ML3 risk probability: `0.801121`
- ML3 prediction: `1`
- ML3 risk level: `HIGH`

### ML5 API Result

- Risk score: `0.801121`
- Risk level: `HIGH`
- Model version: `ml3-logistic-v1`
- Feature timestamp: `2013-11-06 23:00:00`

### Result

**PASS**

The ML5 API reproduced the trained ML3 probability exactly for this test case.

This confirms that the FastAPI endpoint is connected to the trained model rather than the previous prediction stub.

---

## 5. Test 2 — Input Validation

### Test Case

Submitted:

`active_hours = 25`

The valid range is:

`0–24`

### API Response

HTTP validation error:

`Input should be less than or equal to 24`

### Result

**PASS**

The API correctly rejected an invalid ML2 feature value with HTTP 422 validation behavior.

---

## 6. Test 3 — Model Startup Loading

The service imports the ML3 artifact during application startup.

The following checks are performed:

- Model artifact exists.
- Artifact is a dictionary package.
- `model` is present.
- `feature_columns` is present.
- Feature column list is not empty.

Verified model:

`sklearn.pipeline.Pipeline`

### Result

**PASS**

The trained model loads successfully at service startup.

If the model artifact is missing or invalid, the application raises a clear startup error rather than silently serving fake predictions.

---

## 7. API Contract

Endpoint:

`POST /network/predict-risk`

The existing request and response contract was preserved.

No frontend changes were required.

Response fields:

- `grid_id`
- `feature_timestamp`
- `risk_score`
- `risk_level`
- `model_version`
- `explanation_note`

---

## 8. Operational Interpretation

The ML5 risk score represents the trained model's estimated probability of elevated high-activity risk for the next hourly interval.

The API is intended for operational decision support.

A high risk score should trigger investigation of the grid.

It should not be interpreted as proof of congestion, capacity exhaustion, service failure, or a network fault because the dataset does not contain network capacity or outage ground truth.

---

## 9. Test Summary

| Test | Expected | Actual | Status |
|---|---|---|---|
| Real ML3 model inference | Model prediction returned | `0.801121 / HIGH` | PASS |
| ML3 vs ML5 probability | Matching score | `0.801121 = 0.801121` | PASS |
| Model version | `ml3-logistic-v1` | `ml3-logistic-v1` | PASS |
| Invalid `active_hours=25` | HTTP 422 | HTTP 422 validation error | PASS |
| Startup model loading | Successful | Successful | PASS |
| Existing API contract | Unchanged | Unchanged | PASS |

---

## 10. ML5 Conclusion

ML5 is successfully operationalized.

The trained ML3 classifier is loaded safely by FastAPI and is used for real-time inference through the existing `/network/predict-risk` endpoint.

The implementation preserves the existing consumer contract and correctly validates incoming ML2 features.

ML5 is therefore ready for integration into the batch scoring workflow in ML6.
