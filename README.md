# Uber Data Engineering Project

## **Overview**
This project demonstrates an end-to-end data engineering pipeline for analyzing Uber trip data. It leverages tools like Mage for ETL (Extract, Transform, Load), BigQuery for data warehousing, and Looker Studio for creating insightful dashboards. The project was inspired by [Darshil's YouTube tutorial](https://www.youtube.com/watch?v=WpQECq5Hx9g&list=PLBJe2dFI4sgvQTNNkI3ETYJgNPR4CBpFd&index=8), and customized to fit specific analytical goals.

---

## **Technologies Used**
- **Mage**: For ETL pipeline development.
- **BigQuery**: To store and analyze large datasets efficiently.
- **Looker Studio**: For visualizing key metrics and trends.
- **Python**: For scripting and data manipulation.
- **Google Cloud Platform (GCP)**: Hosting BigQuery and Looker Studio.

---

## **Features**
1. **Data Ingestion**:
   - Imported Uber trip data into Mage pipelines for preprocessing.

2. **Data Transformation**:
   - Cleaned and normalized raw data.
   - Created dimensional tables (e.g., `datetime_dim`, `rate_code_dim`).
   - Built a `fact_table` for analytics.

3. **Data Export**:
   - Exported transformed data into BigQuery for efficient querying.

4. **Visualization**:
   - Designed a dynamic dashboard using Looker Studio.
   - Filters for Vendor, Payment Type, Rate Code, and Trip Distance.
   - Summaries of total revenue, average trip distance, and fare amount.
   - Interactive maps for pickup and drop-off locations.

---

## **Pipeline Architecture**
1. **Extract**: Load raw data into Mage.
2. **Transform**: Apply transformations to generate dimension and fact tables.
3. **Load**: Store processed data in BigQuery.
4. **Visualize**: Use Looker Studio for creating interactive dashboards.

---

## **Project Workflow**
1. **Mage Setup**:
   - Built pipelines to transform data into analytics-ready tables.

2. **BigQuery Tables**:
   - `fact_table`: Consolidated facts like revenue and trip distance.
   - `datetime_dim`, `rate_code_dim`, `pickup_location_dim`, etc.: Dimension tables for analytics.

3. **Dashboard Features**:
   - Summary cards for revenue, record count, and average trip distance.
   - Interactive filters for customized insights.
   - Geographic visualization of pickup/drop-off locations.

---

## **Dashboard Insights**
The final dashboard highlights:
- **Revenue Metrics**: Total revenue from trips.
- **Trip Analysis**: Average trip distance and fare amount.
- **Geographic Trends**: Pickup and drop-off hotspots visualized on a map.


---

## **Acknowledgment**
Special thanks to [Darshil](https://www.youtube.com/watch?v=WpQECq5Hx9g&list=PLBJe2dFI4sgvQTNNkI3ETYJgNPR4CBpFd&index=8) for his tutorial that inspired this project.

---

## **How to Reproduce**
1. Clone this repository:
   ```bash
   git clone https://github.com/yourusername/uber-data-engineering.git
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure `io_config.yaml` for your BigQuery and Mage setup.
4. Run the Mage pipeline to process the data.
5. View the dashboard in Looker Studio by connecting to your BigQuery dataset.

---

## **Future Improvements**
- Add support for real-time data ingestion.
- Implement machine learning models for trip prediction or fraud detection.
- Enhance visualizations with more interactive features.

---

## **License**
This project is open-source and available under the [MIT License](LICENSE).

