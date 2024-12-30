import subprocess
import json
import requests
import time

def fetch_data(auth_user, auth_pass):
    """Fetch subjects and topics from Kafka"""
    response = requests.get(
        "<url_schema_registry>:<port_schema_registry/subjects",
        auth=(auth_user, auth_pass),
        verify=True,
    )
    subjects = response.json()
    subjects = [subject.strip() for subject in subjects]

    result = subprocess.run(
        [
            "bash", "-c", "kafka-topics --list --bootstrap-server <url_kafka_broker>:<port_kafka_broker> --command-config <client.property> | grep -v '^_' || true"
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )

    output_string = result.stdout.decode('utf-8').strip()
    topics = output_string.split("\n")
    return subjects, topics

def map_topics_subjects(topics, subjects):
    """Map topics with subjects and create results"""
    results = []
    
    for topic in topics:
        key = None
        value = None
        for subject in subjects:
            cleaned_subject = subject.replace("-value", "").replace("-key", "")
            if cleaned_subject == topic:
                if subject.endswith("-key"):
                    key = subject
                elif subject.endswith("-value"):
                    value = subject
        results.append({"topic": topic, "key": key, "value": value})

    for subject in subjects:
        cleaned_subject = subject.replace("-value", "").replace("-key", "")
        if cleaned_subject not in topics:
            key = None
            value = None
            if subject.endswith("-key"):
                key = subject
            elif subject.endswith("-value"):
                value = subject
            if key or value:
                results.append({"topic": None, "key": key, "value": value})

    results = [r for r in results if any(r.values())]
    return results

def filter_json_data(data):
    """Filter and separate active and inactive schemas"""
    active_schemas = []
    inactive_schemas = []
    
    for item in data:
        topic = item.get('topic')
        key = item.get('key')
        value = item.get('value')
        
        if topic is not None and isinstance(topic, str):
            if (isinstance(key, str) or isinstance(value, str)) and not (key is None and value is None):
                active_schemas.append(item)
        elif topic is None and (isinstance(key, str) or isinstance(value, str)):
            inactive_schemas.append(item)
            
    return active_schemas, inactive_schemas

def save_to_json(data, filename):
    """Save data to JSON file"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

def save_inactive_to_txt(data, filename):
    """Save inactive schemas to text file"""
    with open(filename, 'w') as f:
        for item in data:
            if isinstance(item.get('key'), str):
                f.write(f"{item['key']}\n")
            if isinstance(item.get('value'), str):
                f.write(f"{item['value']}\n")

def delete_schema(schema_name, delete_type, version=None, auth_user=None, auth_pass=None, registry_url=None):
    """Delete a schema with specified parameters"""
    base_url = registry_url or "<url_schema_registry>:<port_schema_registry>"
    permanent = "?permanent=true" if delete_type == "hard" else ""
    
    if version == "all":
        url = f"{base_url}/subjects/{schema_name}{permanent}"
    elif version == "latest":
        url = f"{base_url}/subjects/{schema_name}/versions/latest{permanent}"
    else:
        url = f"{base_url}/subjects/{schema_name}/versions/{version}{permanent}"
    
    try:
        response = requests.delete(
            url,
            auth=(auth_user, auth_pass),
            verify="/var/ssl/private/ca.crt"
        )
        return response.status_code == 200
    except Exception as e:
        print(f"Error deleting schema {schema_name}: {str(e)}")
        return False

def check_schema_file():
    """Check the status of inactive_schemas.txt and return schemas if available"""
    try:
        with open("inactive_schemas.txt", "r") as f:
            schemas = [line.strip() for line in f.readlines()]
            schemas = [s for s in schemas if s]  # Remove empty lines
            
            if not schemas:
                print("\nStatus Check Results:")
                print("-" * 30)
                print("\u2717 No inactive schemas found to delete")
                print("\u2713 File exists but contains no schemas")
                print("\nPossible reasons:")
                print("1. All schemas are currently active")
                print("2. Previous mapping found no inactive schemas")
                print("\nSuggestion: Run the mapping process again to refresh the status")
                return None
                
    except FileNotFoundError:
        print("\nError: Schema File Check")
        print("-" * 30)
        print("\u2717 inactive_schemas.txt not found")
        print("\nPossible reasons:")
        print("1. Mapping process hasn't been run yet")
        print("2. File was deleted or moved")
        print("\nSuggestion: Run the mapping process first to generate the file")
        return None
        
    return schemas

def handle_schema_deletion():
    """Interactive function to handle schema deletion operations"""
    # Check schema file status
    schemas = check_schema_file()
    if schemas is None:
        return

    # Ask for delete operation confirmation
    print("\nSchema Deletion Operations")
    print("-" * 30)
    response = input("Do you want to perform delete operations with the help of this script? (yes/no): ").lower()
    
    if response != "yes":
        print("Operation cancelled. Exiting...")
        return

    # Ask for delete type
    print("\nDelete Operation Types:")
    print("A: Soft Delete (can be restored)")
    print("B: Hard Delete (permanent)")
    delete_type = input("Please select the type of delete operation (A/B): ").upper()

    if delete_type not in ["A", "B"]:
        print("Invalid option selected. Exiting...")
        return

    # Ask for version options
    print("\nDelete Version Options:")
    print("A: All versions")
    print("B: Pick specific version")
    print("C: Latest version only")
    version_choice = input("Which version do you want to delete? (A/B/C): ").upper()

    version_option = None
    if version_choice == "A":
        version_option = "all"
    elif version_choice == "B":
        version_option = input("Enter the version number to delete: ")
    elif version_choice == "C":
        version_option = "latest"
    else:
        print("Invalid version option selected. Exiting...")
        return

    # Configure authentication
    auth_user = "admin"
    auth_pass = "P@ssw0rd"

    # Execute soft delete first if hard delete is selected
    if delete_type == "B":
        print("\nExecuting soft delete before hard delete...")
        delete_type = "soft"

        for schema in schemas:
            print(f"Soft deleting schema: {schema}")
            success = delete_schema(
                schema_name=schema,
                delete_type=delete_type,
                version=version_option,
                auth_user=auth_user,
                auth_pass=auth_pass
            )

            if not success:
                print(f"Failed to soft delete schema: {schema}")
        delete_type = "hard"  # Switch to hard delete after soft delete

    # Summary before deletion
    print("\nDeletion Operation Summary:")
    print("-" * 30)
    print(f"Operation type: {delete_type.upper()} DELETE")
    print(f"Version selection: {version_option}")
    print(f"Total schemas to process: {len(schemas)}")
    confirm = input("\nProceed with deletion? (yes/no): ").lower()
    
    if confirm != "yes":
        print("Operation cancelled. Exiting...")
        return

    # Process deletions
    print("\nProcessing schema deletions...")
    success_count = 0
    fail_count = 0

    for schema in schemas:
        print(f"Processing schema: {schema}")
        success = delete_schema(
            schema_name=schema,
            delete_type=delete_type,
            version=version_option,
            auth_user=auth_user,
            auth_pass=auth_pass
        )
        
        if success:
            success_count += 1
            print(f"Successfully deleted schema: {schema}")
        else:
            fail_count += 1
            print(f"Failed to delete schema: {schema}")
        
        time.sleep(0.5)

    # Final summary
    print("\nDeletion Operation Summary")
    print("-" * 30)
    print(f"Total schemas processed: {len(schemas)}")
    print(f"Successfully deleted: {success_count}")
    print(f"Failed to delete: {fail_count}")

def main():

    print("Welcome to the Schema Operations Script")
    print("Choose an option to proceed:")
    print("1. Check and display schema mapping results only")
    print("2. Full mode: Check, display results, and manage schema deletion")

    choice = input("Enter your choice (1/2): ").strip()

    if choice not in ['1', '2']:
        print("Invalid choice. Exiting...")
        return

    # Configuration
    auth_user = "<user>"
    auth_pass = "<pass>"
    
    
    print("Fetching subjects and topics...")
    subjects, topics = fetch_data(auth_user, auth_pass)

    print("Mapping topics and subjects...")
    results = map_topics_subjects(topics, subjects)

    save_to_json(results, "result_mapping_schema.json")
    print("Results saved to result_mapping_schema.json")

    active_schemas, inactive_schemas = filter_json_data(results)
    save_to_json(active_schemas, "active_schemas.json")
    save_inactive_to_txt(inactive_schemas, "inactive_schemas.txt")

    print("Processing complete. Generated files:")
    print("- active_schemas.json")
    print("- inactive_schemas.txt")

    if choice == '2':
        handle_schema_deletion()
    else:
        print("Check-only mode selected. No deletion operations will be performed.")

if __name__ == "__main__":
    main()


