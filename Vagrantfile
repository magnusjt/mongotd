# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "bento/centos-6.7"
  config.vm.provision :shell, path: "vagrant_bootstrap.sh"
  config.vm.network "forwarded_port", guest: 80, host: 4567

  config.vm.provider "virtualbox" do |v|
    v.name = "mongotd"
    v.memory = 1024
    v.cpus = 2
  end
end