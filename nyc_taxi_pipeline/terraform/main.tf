terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }

  # Remote state. Configure with:
  #   terraform init -backend-config="bucket=<your-tfstate-bucket>"
  backend "gcs" {
    prefix = "tfstate/cabstream"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------------------
# Service account for the Airflow VM (least privilege).
# ---------------------------------------------------------------------------
resource "google_service_account" "airflow_sa" {
  account_id   = "cabstream-airflow"
  display_name = "CabStream Airflow runner"
}

resource "google_project_iam_member" "airflow_bq_data" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_project_iam_member" "airflow_bq_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "airflow_gcs" {
  bucket = google_storage_bucket.data_lake_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# ---------------------------------------------------------------------------
# Data lake bucket.
# ---------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake_bucket" {
  name                        = "${var.project_id}_data_lake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  # Footgun guard: requires explicit -var="allow_destroy=true" to delete data.
  force_destroy = var.allow_destroy

  # Transition stale staging data to NEARLINE instead of deleting it.
  lifecycle_rule {
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
    condition {
      age            = 30
      matches_prefix = ["staging/"]
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

# ---------------------------------------------------------------------------
# BigQuery dataset.
# ---------------------------------------------------------------------------
resource "google_bigquery_dataset" "nyc_taxi_dataset" {
  dataset_id                 = "nyc_taxi_data"
  location                   = var.region
  delete_contents_on_destroy = var.allow_destroy

  lifecycle {
    prevent_destroy = true
  }
}

# ---------------------------------------------------------------------------
# Airflow VM.
# ---------------------------------------------------------------------------
resource "google_compute_instance" "airflow_vm" {
  name         = "airflow-instance"
  machine_type = "e2-standard-2"
  zone         = "${var.region}-a"
  tags         = ["airflow"]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
      size  = 30
    }
  }

  network_interface {
    network = "default"
    access_config {}
  }

  metadata_startup_script = <<-EOF
    #!/usr/bin/env bash
    set -euo pipefail
    sudo apt-get update
    sudo apt-get install -y python3-pip
    # Pinned install via Airflow's official constraints file.
    sudo pip3 install \
      -r /opt/cabstream/requirements.txt \
      -c "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.10.txt"
  EOF

  service_account {
    email  = google_service_account.airflow_sa.email
    scopes = ["cloud-platform"]
  }
}

# ---------------------------------------------------------------------------
# Firewall: only allow Airflow UI from `admin_cidr`.
# ---------------------------------------------------------------------------
resource "google_compute_firewall" "airflow_ui" {
  name          = "allow-airflow-ui"
  network       = "default"
  source_ranges = [var.admin_cidr]
  target_tags   = ["airflow"]

  allow {
    protocol = "tcp"
    ports    = ["8080"]
  }
}
