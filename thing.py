import boto3
import json

# Initialize IoT client (region must match your IoT Core region)
iot = boto3.client('iot', region_name='us-east-2')

# === CONFIGURATION ===
THING_GROUP = "iotea_goon_squad"
POLICY_NAME = "iotea_policy"
NUM_THINGS = 499

# === CREATE THING GROUP (only once) ===
try:
    iot.create_thing_group(thingGroupName=THING_GROUP)
    print(f"Created thing group: {THING_GROUP}")
except iot.exceptions.ResourceAlreadyExistsException:
    print(f"Thing group '{THING_GROUP}' already exists.")

# === CREATE MULTIPLE THINGS ===
for i in range(1, NUM_THINGS + 1):
    thing_name = f"MyThing-{i}"
    print(f"\nCreating {thing_name}...")

    iot.create_thing(thingName=thing_name)
    print(f" - Thing created: {thing_name}")

    cert_response = iot.create_keys_and_certificate(setAsActive=True)
    cert_arn = cert_response['certificateArn']
    cert_id = cert_response['certificateId']
    cert_pem = cert_response['certificatePem']
    private_key = cert_response['keyPair']['PrivateKey']

    print(f" - Certificate ARN: {cert_arn}")

    with open(f"{thing_name}-certificate.pem.crt", "w") as f:
        f.write(cert_pem)
    with open(f"{thing_name}-private.pem.key", "w") as f:
        f.write(private_key)

    iot.attach_policy(
        policyName=POLICY_NAME,
        target=cert_arn
    )
    print(f" - Attached policy: {POLICY_NAME}")

    iot.attach_thing_principal(
        thingName=thing_name,
        principal=cert_arn
    )

    iot.add_thing_to_thing_group(
        thingGroupName=THING_GROUP,
        thingName=thing_name
    )

print("\nAll things created successfully!")
