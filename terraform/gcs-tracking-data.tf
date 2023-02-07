resource "google_storage_bucket" "gcs_data" {
  name     = "tracking-data-${var.project_number}"
  location = "EU"

  force_destroy               = true
  uniform_bucket_level_access = true
}
