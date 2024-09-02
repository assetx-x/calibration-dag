resource "google_compute_instance" "calibration-ax" {
  project = "ax-prod-393101"

  boot_disk {
    auto_delete = true
    device_name = "instance-20240902-000650"

    initialize_params {
      image = "projects/debian-cloud/global/images/debian-12-bookworm-v20240815"
      size  = 500
      type  = "pd-standard"
    }

    mode = "READ_WRITE"
  }

  can_ip_forward      = false
  deletion_protection = false
  enable_display      = false

  labels = {
    goog-ec-src = "vm_add-tf"
  }

  machine_type = "e2-highmem-8"

  metadata = {
    ssh-keys = <<-EOT
      iklo:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCyPnxThY1CZf9vqv9i9WlIumqBAuOIo/1UxwrEvSXxNHgoTKXzaLCQfw3JWtApdnTqo0GRvPcVby44DJUlBEywwraWgUo317VSyFP7CDkz2zvG07NWTYLK4COQPejFqFLNFfHTpIOAEz1WYpCwlXOYtUQk3BGORZ+adKzxbt/tFYYC8ymbgQz8MjooUQQiKt/YFejqw+i2vmbl9mnTmRZoDSdeK3EhLWlLXHbNggjlR9eu17puo3V7UO9c4SqaJuBzoSX9Mfaq9SuZtjAld93U6WCGuiUlqDcJc7Bdu+mjAURmWx8hWj9muPlAAg+mEdOzkHrvk49ztuRke+IeDoxRvBYkjTCRlV8tChYRdwP3m1QgUHX79kP2IJNdQ9KNC6p+2z/AlLRWqhbgJ99qZqbA5f2FSUe5nHuv+9Jd0oqprbDN0JXR0UGn0YNN9FQzN0WrpCtLEEGPTcRCi46Asbtr6AKMFnIzLU5lxubvCPYT59Zkt8suBO1d4znachMX14s= iklo@iklos-MacBook-Pro.local
      alexanderjohnson:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABgQCxOFiDDYoHUuN7+HZbL8sVfIzAC99Yhf3Q40L6+tJZLsmgf8gLscRaRRtyJ315ZLI17W7Bm98bFsNcRup35/rR6zDZ9Wk11OU9nMtCp4fpLpzp/J+wfDFgjvYZIaWpf/FXEga95bXCd1z4HOBSzVy5kuNbRENfm0P5Flp1sGVQwy8ywclDG80y0Toa/DOnFG1oab66k+hEeFsf59G1TDuiFQINm+ZbROdcmS9rmSNigLTh1itlaLah3HkRN/v84JRWxc9T8MxLymSG2Xk75pSO492C13A/w7x/cQJsyH6Jj6U7mVpHB9Qwastjm/wSCZGmX+aHLoLvUK597ssUn0LgeFMkLbbguqag4/O7Ma8fM2EeaZsSS8jBcS/VS9D2uPZD7ajuPSOvUyXLya72mLmCNLAW70Dvj/rWlZh53aqnq/mYdA+mpOYfVqaCKDA9rLxPlF0XYjE5vAAZPNr9kePKwkdOxDYM//Fsat7mE3mdFSs46CBZrYxE4kMdc9SXBYM= alexanderjohnson@Alexanders-MBP.lan
    EOT

    startup-script = <<-EOT
      #!/bin/bash
      sudo apt-get update
      sudo apt-get install -y ca-certificates curl htop jq lsb-release net-tools unzip vim wget zip make

      ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -q -N ""

      sudo install -m 0755 -d /etc/apt/keyrings
      sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
      sudo chmod a+r /etc/apt/keyrings/docker.asc
      echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
        $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
        sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
      sudo apt-get update
      sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
      sudo groupadd docker
      sudo usermod -aG docker $USER
    EOT
  }

  name = "calibration-ax"

  network_interface {
    access_config {
      network_tier = "PREMIUM"
    }

    queue_count = 0
    stack_type  = "IPV4_ONLY"
    subnetwork  = "projects/ax-prod-393101/regions/us-central1/subnetworks/default"
  }

  scheduling {
    automatic_restart   = true
    on_host_maintenance = "MIGRATE"
    preemptible         = false
    provisioning_model  = "STANDARD"
  }

  service_account {
    email  = "1058145182637-compute@developer.gserviceaccount.com"
    scopes = [
      "https://www.googleapis.com/auth/devstorage.read_only",
      "https://www.googleapis.com/auth/logging.write",
      "https://www.googleapis.com/auth/monitoring.write",
      "https://www.googleapis.com/auth/service.management.readonly",
      "https://www.googleapis.com/auth/servicecontrol",
      "https://www.googleapis.com/auth/trace.append"
    ]
  }

  shielded_instance_config {
    enable_integrity_monitoring = true
    enable_secure_boot          = false
    enable_vtpm                 = true
  }

  tags = ["http-server", "https-server"]
  zone = "us-central1-c"
}

output "instance_public_ip" {
  value       = google_compute_instance.calibration-ax.network_interface[0].access_config[0].nat_ip
  description = "The public IP address of the calibration-ax instance"
}

output "ssh_public_key" {
  value       = file("~/.ssh/id_rsa.pub")
  description = "The SSH public key of the calibration-ax instance"
}
