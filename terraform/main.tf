terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Consider using "gcs" for Google Cloud or "s3" for AWS to manage state files in the cloud
  required_providers {
    google = {
      source = "hashicorp/google"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials)
}

locals {
  common_tags = {
    project     = var.project_id
    managed_by  = "terraform"
    environment = terraform.workspace
  }
}

# GCS Buckets for ETL
resource "google_storage_bucket" "etl_raw" {
  name                        = "${var.project_id}-etl-raw-${terraform.workspace}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  labels                      = local.common_tags
  versioning {
    enabled = true
  }
  force_destroy = true
}

resource "google_storage_bucket" "etl_processed" {
  name                        = "${var.project_id}-etl-processed-${terraform.workspace}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  labels                      = local.common_tags
  versioning {
    enabled = true
  }
  force_destroy = true
}

resource "google_storage_bucket" "etl_archive" {
  name                        = "${var.project_id}-etl-archive-${terraform.workspace}"
  location                    = var.region
  storage_class               = "ARCHIVE"
  uniform_bucket_level_access = true
  labels                      = local.common_tags
  versioning {
    enabled = true
  }
  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 365  # days
    }
  }
  force_destroy = true
}

# BigQuery Datasets for ETL
resource "google_bigquery_dataset" "etl_src" {
  dataset_id = "etl_src"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "etl_ods" {
  dataset_id = "etl_ods"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "etl_dim" {
  dataset_id = "etl_dim"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "etl_fact" {
  dataset_id = "etl_fact"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "etl_mart" {
  dataset_id = "etl_mart"
  project    = var.project_id
  location   = var.region
}

# IAM for Airflow Service Account
resource "google_service_account" "airflow" {
  account_id   = "airflow-account"
  display_name = "Airflow Service Account"
}

resource "google_project_iam_member" "airflow_gcs_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_gcs_creator" {
  project = var.project_id
  role    = "roles/storage.objectCreator"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_gcs_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_connection_user" {
  project = var.project_id
  role    = "roles/bigquery.connectionUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_project_iam_member" "airflow_bq_connection_delegate" {
  project = var.project_id
  role    = "roles/bigquery.connectionAdmin"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}

resource "google_service_account" "viewer_sa" {
  account_id   = "viewer-account"
  display_name = "Viewer Service Account"
}

resource "google_project_iam_member" "viewer_sa_viewer" {
  project = var.project_id
  role    = "roles/viewer"
  member  = "serviceAccount:${google_service_account.viewer_sa.email}"
}

resource "google_storage_bucket" "cloud_function_bucket" {
  name = "${var.project_id}-${var.function_name}-${terraform.workspace}"
  location = var.region
  uniform_bucket_level_access = true
  force_destroy = true
}

data "archive_file" "src" {
  type = "zip"
  source_dir = "${path.root}/src/"  # Directory where the function code is located
  output_path = "${path.root}/generated/${var.function_name}.zip"
  #output_path = "${google_storage_bucket.cloud_function_bucket.name}/${var.function_name}.zip"
}

resource "google_storage_bucket_object" "archive" {
  name = "${data.archive_file.src.output_md5}.zip"
  bucket = google_storage_bucket.cloud_function_bucket.name
  source = data.archive_file.src.output_path
  metadata = {
    managed_by = "terraform"
    environment = terraform.workspace
  }
}

resource "google_cloudfunctions2_function" "emotion_analyzer" {
    name = var.function_name
    location = var.region
    description = "Emotion Analyzer Function"

    build_config {
      runtime     = "python310"
      entry_point = "analyze_sentiment"  # Set the entry point
      source {
        storage_source {
          bucket = google_storage_bucket.cloud_function_bucket.name
          object = google_storage_bucket_object.archive.name
        }
      }
    }

    service_config {
      min_instance_count    = 1
      max_instance_count    = 10
      available_cpu         = "1"
      available_memory      = "2Gi"
      max_instance_request_concurrency = 80
      timeout_seconds       = 300
    }

    labels = local.common_tags
    depends_on = [
      google_storage_bucket.cloud_function_bucket,
      google_storage_bucket_object.archive
    ]
  }

resource "google_cloudfunctions2_function_iam_member" "emotion_analyzer_invoker" {
  project        = google_cloudfunctions2_function.emotion_analyzer.project
  location       = google_cloudfunctions2_function.emotion_analyzer.location
  cloud_function = google_cloudfunctions2_function.emotion_analyzer.name
  role           = "roles/cloudfunctions.invoker"
  member         = "serviceAccount:${google_bigquery_connection.emotion_analyzer_connection.cloud_resource[0].service_account_id}"
}

resource "google_cloud_run_service_iam_member" "emotion_analyzer_run_invoker" {
  project        = google_cloudfunctions2_function.emotion_analyzer.project
  location       = google_cloudfunctions2_function.emotion_analyzer.location
  service        = google_cloudfunctions2_function.emotion_analyzer.name
  role           = "roles/run.invoker"
  member         = "serviceAccount:${google_bigquery_connection.emotion_analyzer_connection.cloud_resource[0].service_account_id}"
}

resource "google_bigquery_connection" "emotion_analyzer_connection" {
    connection_id = var.function_name
    project = var.project_id
    location = var.region
    cloud_resource {}
}
