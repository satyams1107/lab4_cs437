import os

# Path formatters — match your script
certificate_formatter = "thing-private/MyThing-{}-certificate.pem.crt"
key_formatter = "thing-private/MyThing-{}-private.pem.key"

# Device range
device_st = 1
device_end = 499

# Keep track of missing files
missing_certs = []
missing_keys = []

for device_id in range(device_st, device_end + 1):
    cert_path = certificate_formatter.format(device_id)
    key_path = key_formatter.format(device_id)

    if not os.path.isfile(cert_path):
        missing_certs.append(cert_path)
    if not os.path.isfile(key_path):
        missing_keys.append(key_path)

# Report
if not missing_certs and not missing_keys:
    print("✅ All certificate and key files exist!")
else:
    if missing_certs:
        print("❌ Missing certificate files:")
        for c in missing_certs:
            print("   ", c)
    if missing_keys:
        print("❌ Missing private key files:")
        for k in missing_keys:
            print("   ", k)

python3 basic_discovery.py \
  --thing_name MyClientDevice1 \
  --topic 'vehicle/emission/data' \
  --message 'PLZ WORK!' \
  --ca_file ~/certs/AmazonRootCA1.pem \
  --cert ~/certs/device.pem.crt \
  --key ~/certs/private.pem.key \
  --region us-east-2 \
  --verbosity Warns

python3 basic_discovery.py \
  --thing_name MyThing-1 \
  --topic 'vehicle/emission/data' \
  --message 'PLZ WORK!' \
  --ca_file ~/certs/AmazonRootCA1.pem \
  --cert ~/certs/MyThing-1-certificate.pem.crt \
  --key ~/certs/MyThing-1-private.pem.key \
  --region us-east-2

/home/satyam/iotea/EmissionAnalyzer/greengrass-build/artifacts/com.iotea.EmissionAnalyzer/1.0.3/src.zip
# Copy the certificate
scp -i iotea.pem 7e618fd2d5814392cf3ac0e2aa4c73df8f7190941347ca90f123c48ba338ed08-certificate.pem.crt ec2-user@ec2-3-133-85-16.us-east-2.compute.amazonaws.com:~

# Copy the private key
scp -i iotea.pem 7e618fd2d5814392cf3ac0e2aa4c73df8f7190941347ca90f123c48ba338ed08-private.pem.key ec2-user@ec2-3-133-85-16.us-east-2.compute.amazonaws.com:~

# Copy the Root CA
scp -i iotea.pem AmazonRootCA1.pem ec2-user@ec2-3-133-85-16.us-east-2.compute.amazonaws.com:~

scp -i ../iotea.pem \
  ./greengrass-build/artifacts/com.iotea.EmissionAnalyzer/1.0.3/src.zip \
  ec2-user@ec2-3-133-85-16.us-east-2.compute.amazonaws.com:~/