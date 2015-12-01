sudo su
cd ~

# Set timezone to use here
ln -sf /usr/share/zoneinfo/Europe/Oslo /etc/localtime

# Download updated yum repositories (epel and remi for php 5.5)
wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
wget http://rpms.famillecollet.com/enterprise/remi-release-6.rpm
rpm -Uvh remi-release-6*.rpm epel-release-6*.rpm

# Create yum repo for mongodb
echo "
[mongodb-org-3.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/\$releasever/mongodb-org/3.0/x86_64/
gpgcheck=0
enabled=1" > /etc/yum.repos.d/mongodb-org-3.0.repo

# Install libs needed to pecl install mongo
yum -y install gcc-c++ openssl-devel cyrus-sasl-devel

# Install mongodb server
yum -y install mongodb-org

# Install php + libs
yum -y --enablerepo=remi,remi-php55 install httpd php php-pear php-common php-devel php-cli php-pecl-xdebug php-pdo php-opcache php-mbstring php-mcrypt php-xml

# Install php mongo driver
pecl install mongo

# Enable mongo extension
echo "
extension=mongo.so" >> /etc/php.ini

# Add config to enable xdebug extension (but don't enable it by default)
echo "
;[xdebug]
;zend_extension=xdebug.so
;xdebug.remote_host=10.0.2.2
;xdebug.remote_enable=on
;xdebug.remote_connect_back=off
;xdebug.profiler_enable=off
;xdebug.profiler_output_dir=/vagrant/profiles" >> /etc/php.ini

# Disable xdebug by default:
rm /etc/php.d/xdebug.ini

# Copy our mongod config before starting mongod for the first time
cp /vagrant/mongod.conf /etc/mongod.conf

# Start services
service httpd start
service mongod start

# Make sure services start on system startup
chkconfig httpd on
chkconfig mongod on

# Link test/functional/web to web page found at /web
ln -s /vagrant/test/functional/web /var/www/html/web

cd /tmp
curl -sS https://getcomposer.org/installer | php
mv /tmp/composer.phar /usr/local/bin/composer