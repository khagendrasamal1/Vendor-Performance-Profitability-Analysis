# Vendor Performance & Profitability Analysis

## 📌 Project Overview
An end-to-end data engineering and analytics pipeline designed to evaluate retail vendor efficiency. This project transitions from raw transactional data (1M+ rows) to a high-level Power BI dashboard, validated by statistical hypothesis testing.

## 🚀 Key Features
- **Scalable Ingestion:** Python pipeline using SQLAlchemy to move 1M+ records into SQL with 60% memory optimization.
- **Relational Modeling:** Advanced SQL architecture (CTEs/JOINs) to engineer KPIs like Stock Turnover and Profit Margin.
- **Statistical Rigor:** Conducted Two-Sample T-Tests ($p < 0.05$) to validate profitability variances across vendor segments.
- **Business Intelligence:** Interactive Power BI dashboard identifying top-performing vendors and inventory optimization targets.

## 🛠️ Tech Stack
- **Languages:** Python (Pandas, Scipy, SQLAlchemy)
- **Database:** SQLite / SQL
- **Visualization:** Power BI, Seaborn
- **Environment:** Jupyter Notebook

## 📊 Business Insights
- Identified that 20% of vendors contribute to 80% of total gross profit (Pareto Principle).
- Detected high-volume vendors with low turnover rates, pinpointing potential "dead stock" holding costs.
- Statistically proved that top-tier vendor segments maintain significantly higher margins, justifying volume-based pricing strategies.
