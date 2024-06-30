resource "google_project_service" "composer_api" {
  project  = var.project_id
  service  = "composer.googleapis.com"

  disable_on_destroy = false
}

# resource "google_service_account" "composer_sa" {
#   account_id   = "composer-account"
#   display_name = "Composer Service Account"
# }

resource "google_project_iam_member" "composer_sa" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${var.airflow_sa_email}"
  # member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_composer_environment" "gmaps_composer_env" {
  name     = "gmaps-composer"
  
  config {
    software_config {
      image_version = "composer-3-airflow-2.7.3-build.6"
    }

    workloads_config {
      scheduler {
        cpu        = var.scheduler_cpu
        memory_gb  = var.scheduler_memory_gb
        storage_gb = var.scheduler_storage_gb
      }
      web_server {
        cpu        = var.web_server_cpu
        memory_gb  = var.web_server_memory_gb
        storage_gb = var.web_server_storage_gb
      }
      worker {
        cpu        = var.worker_cpu
        memory_gb  = var.worker_memory_gb
        storage_gb = var.worker_storage_gb
        min_count  = var.worker_min_count
        max_count  = var.worker_max_count
      }
    }
    environment_size = var.environment_size
  }

  labels = var.common_tags

  depends_on = [google_project_service.composer_api]
}
