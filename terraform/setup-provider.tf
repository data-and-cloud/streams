provider "google" {
  project = var.project
}

terraform {
  backend "gcs" {
  }
}
