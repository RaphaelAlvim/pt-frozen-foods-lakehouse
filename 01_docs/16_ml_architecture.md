# 16. ML Architecture

## 1. Overview

The PT Frozen Foods platform was designed to support both analytical and predictive workloads within a unified enterprise-grade Lakehouse architecture.

After consolidating the ingestion, transformation, analytics and BI layers, the platform now evolves to support Machine Learning workflows integrated directly into the existing data platform.

The ML layer extends the current architecture and enables:

- feature engineering
- model training
- experiment tracking
- prediction generation
- predictive analytics
- future MLOps evolution
- integration between data engineering and data science workloads

The objective of this phase is to incorporate predictive capabilities into the existing governed platform while preserving:

- scalability
- maintainability
- governance
- cost efficiency
- operational consistency

---

# 2. ML Layer Positioning

The ML layer is not treated as an isolated platform.

Instead, it operates as an extension of the existing Lakehouse architecture and consumes curated analytical datasets already available in the platform.

Current architecture:

    RAW
    ↓
    Bronze
    ↓
    Silver
    ↓
    Gold
    ↓
    Analytics
    ↓
    ML
    ↓
    BI

Layer responsibilities:

- Bronze handles ingestion
- Silver handles standardization and integration
- Gold handles dimensional modeling
- Analytics handles optimized analytical consumption
- ML handles predictive workloads
- BI handles business visualization and consumption

The ML layer depends directly on the quality, governance and consistency of upstream datasets.

---

# 3. Initial ML Scope

The initial ML implementation focuses on a single business use case.

Selected use case:

## Sales Forecasting

Reasons for selection:

- strong business value
- direct relationship with existing datasets
- natural integration with historical sales data
- compatibility with the current analytics layer
- strong potential for operational and strategic insights
- direct integration with Power BI dashboards

The implementation focuses on building a production-oriented forecasting pipeline integrated into the current platform architecture.

---

# 4. Future ML Possibilities

The platform architecture was designed to support future ML expansion.

Potential future use cases include:

## Customer Segmentation

Possible techniques:

- clustering
- behavioral segmentation
- RFM analysis

Potential business value:

- customer targeting
- marketing optimization
- commercial segmentation
- customer behavior analysis

---

## Churn Prediction

Possible techniques:

- binary classification
- probability scoring

Potential business value:

- customer retention
- proactive commercial actions
- revenue preservation

---

## Recommendation System

Possible techniques:

- collaborative filtering
- association rules
- hybrid recommendation models

Potential business value:

- cross-selling
- upselling
- basket optimization

The architecture already anticipates future expansion without requiring structural redesign.

---

# 5. ML Architecture Flow

Official ML architecture flow:

    Gold / Analytics
    ↓
    Feature Engineering
    ↓
    ML Dataset
    ↓
    Training
    ↓
    Predictions
    ↓
    Power BI Consumption

The ML layer consumes curated datasets from:

- `gold.fact_sales`
- `gold.analytics_sales_overview`
- future analytical feature tables

The forecasting pipeline operates on governed Delta datasets already standardized by the platform.

---

# 6. Technology Stack

The Machine Learning layer uses Azure Databricks as the primary ML platform.

Main technologies:

| Component | Technology |
|---|---|
| Processing | PySpark |
| Exploratory Analysis | Pandas |
| ML Frameworks | scikit-learn / XGBoost / LightGBM |
| Experiment Tracking | MLflow |
| Storage | Delta Lake |
| Governance | Unity Catalog |
| Visualization | Power BI |
| Orchestration | Azure Data Factory |
| Infrastructure | Terraform |

---

# 7. Spark vs Pandas Strategy

The platform adopts a hybrid processing strategy.

## PySpark Responsibilities

PySpark is used for:

- distributed transformations
- scalable feature engineering
- Delta materialization
- large-scale aggregations
- processing orchestration

---

## Pandas Responsibilities

Pandas is used for:

- exploratory analysis
- local modeling workflows
- smaller aggregated ML datasets
- model evaluation
- experimentation

This approach preserves scalability while avoiding unnecessary distributed complexity during model experimentation.

---

# 8. ML Notebook Strategy

The ML layer follows the same organizational standards adopted across the entire platform.

Official structure:

    04_notebooks/06_ml/
    ├── README.md
    ├── exploratory/
    ├── processing/
    ├── training/
    ├── inference/
    └── monitoring/

---

# 9. Exploratory Notebooks

Exploratory notebooks are used for:

- EDA
- feature analysis
- model experimentation
- hypothesis validation
- model comparison

Characteristics:

- notebook-based
- iterative
- visualization-oriented
- experimentation-friendly

These notebooks are not treated as production artifacts.

---

# 10. Processing Notebooks

Processing notebooks are responsible for:

- reusable feature engineering
- Delta table generation
- deterministic transformations
- scheduled execution

Characteristics:

- `.py` notebooks
- minimalistic structure
- production-oriented execution
- no interactive visualization
- optimized for performance and cost

The same standards already adopted in Bronze, Silver, Gold and Analytics layers are preserved in the ML processing layer.

---

# 11. Feature Engineering Strategy

Feature engineering is one of the most critical stages of the forecasting workflow.

Potential forecasting features include:

- historical sales
- moving averages
- lag features
- seasonality indicators
- day of week
- month
- holiday proximity
- customer behavior metrics
- product behavior metrics
- sales channel behavior
- weather indicators

Feature engineering outputs may later be materialized as dedicated Delta tables.

Possible future tables:

- `gold.ml_sales_forecasting_features`

---

# 12. Model Training Strategy

The initial implementation prioritizes:

- reproducibility
- interpretability
- operational simplicity
- maintainability

Initial modeling sequence:

1. Baseline model
2. Linear models
3. Tree-based models
4. Gradient boosting models

Potential frameworks:

- scikit-learn
- XGBoost
- LightGBM

The objective is to establish a robust forecasting workflow integrated into the existing platform architecture.

---

# 13. MLflow Strategy

MLflow is used for:

- experiment tracking
- metric comparison
- parameter logging
- model versioning

Potential tracked elements:

- MAE
- RMSE
- MAPE
- training parameters
- feature sets
- model versions

This ensures reproducibility and governance across the ML lifecycle.

---

# 14. Predictions Strategy

Predictions generated by the forecasting models may later be materialized as Delta tables.

Possible table:

- `gold.ml_sales_forecasting_predictions`

Potential contents:

- prediction date
- forecast horizon
- predicted sales
- confidence metrics
- product
- channel
- customer segment

Predictions become governed analytical assets consumable by downstream systems.

---

# 15. Power BI Integration

Power BI is responsible for consuming predictive outputs already materialized in Delta tables.

Potential future dashboards:

- Forecast vs Actual
- Revenue Projection
- Sales Trend Forecast
- Product Demand Forecast
- Channel Forecast Performance

The architecture preserves clear separation between:

- data platform
- ML workloads
- analytical visualization

---

# 16. Governance and Unity Catalog

ML assets remain governed under Unity Catalog.

Potential governed assets:

- feature tables
- prediction tables
- ML datasets
- registered models

Benefits:

- centralized governance
- lineage
- auditing
- access control
- environment consistency

---

# 17. Initial MLOps Vision

The current implementation focuses on foundational ML architecture integrated into the existing platform.

The architecture already anticipates future MLOps evolution.

Possible future lifecycle:

    DEV
    ↓
    TEST
    ↓
    MODEL VALIDATION
    ↓
    MODEL REGISTRY
    ↓
    BATCH INFERENCE
    ↓
    PRODUCTION CONSUMPTION

Future platform evolution may include:

- scheduled retraining
- automated inference
- monitoring
- drift detection
- CI/CD integration
- lifecycle management

---

# 18. Initial Scope Limitations

The current ML phase intentionally does not include:

- real-time inference
- online serving
- streaming ML
- advanced MLOps automation
- distributed hyperparameter optimization at scale
- production-grade serving endpoints

These capabilities may be incorporated in future platform evolutions if business requirements demand them.

---

# 19. Architectural Decision Summary

The platform officially adopts:

- Azure Databricks as the ML platform
- PySpark for scalable processing
- Pandas for exploratory ML workflows
- MLflow for experiment tracking
- Delta Lake for ML datasets and predictions
- Power BI for predictive analytics consumption
- Sales Forecasting as the first production ML use case

This approach preserves:

- scalability
- governance
- maintainability
- operational consistency
- architectural cohesion

while enabling the platform to support predictive workloads integrated directly into the Lakehouse architecture.

---

# 20. Conclusion

The PT Frozen Foods platform now evolves from a traditional analytical architecture into a unified analytical and predictive data platform.

The ML layer was designed to integrate naturally into the existing governed architecture while preserving the same operational, governance and performance standards already established across the platform.

The resulting architecture supports:

- analytical workloads
- predictive workloads
- governed ML assets
- scalable feature engineering
- reproducible experimentation
- integrated business consumption

inside a single enterprise-oriented Lakehouse platform.