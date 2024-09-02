provider "google" {
  project = "dcm-prod-ba2f"
  region  = "us-east4"
}

resource "google_compute_instance" "ai_calibration" {
  name         = "ai-calibration"
  machine_type = "c2-standard-60"
  zone         = "us-east4-b"

  boot_disk {
    initialize_params {
      image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts"
      size  = 1000
    }

#     guest_os_features {
#       type = "VIRTIO_SCSI_MULTIQUEUE"
#     }
#
#     guest_os_features {
#       type = "SEV_CAPABLE"
#     }
#
#     guest_os_features {
#       type = "UEFI_COMPATIBLE"
#     }
#
#     guest_os_features {
#       type = "GVNIC"
#     }
  }

  network_interface {
    network    = "projects/dcm-prod-ba2f/global/networks/dcm-public-vpc"
    subnetwork = "projects/dcm-prod-ba2f/regions/us-east4/subnetworks/dcm-public-sn14"
    network_ip = "10.14.0.11"
    access_config {
      # Include this block to add an external IP
    }
  }

  metadata = {
    startup-script = <<-EOT
      #!/bin/bash
      BASE_INSTALL_LOG=/tmp/base_install_$(date +'%Y%m%d-%H%M%S').log
      echo "$(date +'%Y%m%d-%H%M%S') $0 Starting" >> ${BASE_INSTALL_LOG}

      DCM_ROOT_USER=dcmadmin
      /usr/sbin/useradd -c"DCM Admin" -s/bin/bash -m -Gsudo,google-sudoers ${DCM_ROOT_USER}

      SSH_ROOT_DIR=/home/${DCM_ROOT_USER}/.ssh
      mkdir ${SSH_ROOT_DIR}
      chmod 700 ${SSH_ROOT_DIR}
      echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDXsA4AEZPPb+WnZy3ui+cQ0RyHYuqtpsfQkK8CcodQ3jHa/cXICDpjczslt6mU38HNSk+aQGI+e8jkglGzBcBNid+xqIaZd4RD3bKYpGofFRCNDMAd4je7kNuYuK2LedcrLlBd34dw1t80s1E0uY6P68SO4Be/wiEwMEzs46/38X9jM6d7lAXIgPstY7S94pcshuWimGM90913n+/AaNge+5eyiBC0Hm1bZ+iJJFRu0W/3/b8DFeYsRr5qtk/PDM7kvzucQ0whecLhYaYPM8hbkPTQIg5wl62CfWIw80ZiFV8Nk9Yj3YEy4Ld/YmifjHnAZzN/zJtizpo43yYMscRb dcm-compute-keypair01" >> ${SSH_ROOT_DIR}/authorized_keys
      cat <<EOF > ${SSH_ROOT_DIR}/id_rsa
      -----BEGIN RSA PRIVATE KEY-----
      [REDACTED_PRIVATE_KEY]
      -----END RSA PRIVATE KEY-----
      EOF

      chmod 600 ${SSH_ROOT_DIR}/authorized_keys
      chmod 600 ${SSH_ROOT_DIR}/id_rsa
      chown -R ${DCM_ROOT_USER} ${SSH_ROOT_DIR}

      apt -qq update
      apt -qq -y upgrade
      apt-add-repository --yes --update ppa:ansible/ansible
      apt -qq install --yes software-properties-common ansible

      echo "$(date +'%Y%m%d-%H%M%S') $0 Completed" >> ${BASE_INSTALL_LOG}
    EOT
    ssh-keys = <<-EOT
      dcm-compute-keypair01:ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDXsA4AEZPPb+WnZy3ui+cQ0RyHYuqtpsfQkK8CcodQ3jHa/cXICDpjczslt6mU38HNSk+aQGI+e8jkglGzBcBNid+xqIaZd4RD3bKYpGofFRCNDMAd4je7kNuYuK2LedcrLlBd34dw1t80s1E0uY6P68SO4Be/wiEwMEzs46/38X9jM6d7lAXIgPstY7S94pcshuWimGM90913n+/AaNge+5eyiBC0Hm1bZ+iJJFRu0W/3/b8DFeYsRr5qtk/PDM7kvzucQ0whecLhYaYPM8hbkPTQIg5wl62CfWIw80ZiFV8Nk9Yj3YEy4Ld/YmifjHnAZzN/zJtizpo43yYMscRb dcm-compute-keypair01
      # Add other ssh-keys here
    EOT
  }

  tags = ["http", "ssh-enabled"]

  service_account {
    email  = "compute-instance@dcm-prod-ba2f.iam.gserviceaccount.com"
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }

  scheduling {
    on_host_maintenance  = "MIGRATE"
    automatic_restart    = true
    preemptible          = false
  }

  shielded_instance_config {
    enable_secure_boot          = false
    enable_vtpm                  = true
    enable_integrity_monitoring  = true
  }
}
