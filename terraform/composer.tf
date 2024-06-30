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
  member  =  "serviceAccount:${google_service_account.airflow.email}"
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
        cpu        = 1
        memory_gb  = 4
        storage_gb = 10
      }
      web_server {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 10
      }
      worker {
        cpu        = 1
        memory_gb  = 4
        storage_gb = 10
      }
    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"
  }

  labels = local.common_tags

  depends_on = [google_project_service.composer_api]
}
