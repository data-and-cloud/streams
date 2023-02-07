resource "google_bigquery_dataset" "tracking_data" {
  project    = var.project
  location   = "EU"
  dataset_id = "tracking_data"
}

resource "google_bigquery_table" "raw" {
  dataset_id = google_bigquery_dataset.tracking_data.dataset_id
  table_id   = "raw"

  deletion_protection = false

  time_partitioning {
    field                    = "event_time"
    type                     = "DAY"
  }

  schema = file("bigquery_tracking_data_schema.json")
}
