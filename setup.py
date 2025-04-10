import os
import subprocess
from setuptools import setup, find_packages
from setuptools.command.build_py import build_py as _build_py
from urllib.parse import urlparse
from pathlib import Path
import logging
import shutil

import requests

current_path = os.path.dirname(os.path.realpath(__file__))

DAS_VERSION = "RD-15492-add-http-headers-and-scopes-to-protocol-das"

PROTOS = [
    "com/rawlabs/protocol/das/v1/common/das.proto",
    "com/rawlabs/protocol/das/v1/common/environment.proto",
    "com/rawlabs/protocol/das/v1/functions/functions.proto",
    "com/rawlabs/protocol/das/v1/tables/tables.proto",
    "com/rawlabs/protocol/das/v1/types/types.proto",
    "com/rawlabs/protocol/das/v1/types/values.proto",
    "com/rawlabs/protocol/das/v1/query/quals.proto",
    "com/rawlabs/protocol/das/v1/query/query.proto",
    "com/rawlabs/protocol/das/v1/query/operators.proto",
    "com/rawlabs/protocol/das/v1/services/health_service.proto",
    "com/rawlabs/protocol/das/v1/services/registration_service.proto",
    "com/rawlabs/protocol/das/v1/services/tables_service.proto",
    "com/rawlabs/protocol/das/v1/services/functions_service.proto"
]

logging.basicConfig(
    filename='setup.log',  # Log to a file (you can change this to stdout if preferred)
    level=logging.DEBUG,  # Set log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

logger.info(f"Current path: {current_path}")

local_proto_files = os.getenv("LOCAL_PROTO_FILES")
if local_proto_files:
    PROTO_FILES = [f"{local_proto_files}/src/main/protobuf/{proto}" for proto in PROTOS]
else:
    PROTO_FILES = [f"https://raw.githubusercontent.com/raw-labs/protocol-das/{DAS_VERSION}/src/main/protobuf/{proto}" for proto in PROTOS]

downloaded_proto_dir = os.path.join(current_path, "downloaded")


class PrepareGrpcPackages:
    
    def run(self):
        self.create_downloaded_proto_dir()
        self.download_proto_files()
        self.generate_grpc_code()
        self.add_init_py_to_com_folders()

    def create_downloaded_proto_dir(self):
        """Ensure the local proto directory exists."""
        os.makedirs(downloaded_proto_dir, exist_ok=True)

    def download_proto_files(self):
        """Download proto files and recreate the directory structure."""
        for proto_file_url in PROTO_FILES:
            local_file_path = self.get_local_file_path(proto_file_url)
            if not local_file_path:
                continue

            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
            if proto_file_url.startswith("http"):
                logger.info(f"Downloading {proto_file_url} to {local_file_path}")
                self.download_file(proto_file_url, local_file_path)
            else:
                logger.info(f"Copying {proto_file_url} to {local_file_path}")
                shutil.copyfile(proto_file_url, local_file_path)

    def get_local_file_path(self, proto_file_url):
        """Parse the URL to get the path and filename, and recreate local structure."""
        logger.info(f"Getting local file path for {proto_file_url}")
        parsed_url = urlparse(proto_file_url)
        proto_path_parts = parsed_url.path.split('/')

        try:
            com_index = proto_path_parts.index('com')
        except ValueError:
            return None

        return os.path.join(downloaded_proto_dir, *proto_path_parts[com_index:])

    def download_file(self, url, local_path):
        """Download the file from the given URL and save it locally."""
        logger.info(f"Downloading {url} to {local_path}")
        response = requests.get(url)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            logger.info(f"Downloaded {url} to {local_path}")
            return True
        else:
            logger.error(f"Failed to download {url}")
            return False

    def generate_grpc_code(self):
        """Generate gRPC Python code from all proto files."""
        for proto_file in self.find_proto_files():
            self.run_protoc(proto_file)

    def find_proto_files(self):
        """Find all .proto files in the local proto directory."""
        for root, _, files in os.walk(downloaded_proto_dir):
            logger.info(f"Searching for .proto files in {root}")
            for file in files:
                logger.info(f"Found file: {file}")
                if file.endswith('.proto'):
                    yield os.path.join(root, file)

    def run_protoc(self, proto_file):
        """Run the protoc command to generate Python gRPC code for a given proto file."""
        logger.info(f"Running protoc for {proto_file}")
        try:
            logger.debug(f"python3 -m grpc_tools.protoc -I={downloaded_proto_dir} --python_out={current_path} --grpc_python_out={current_path} {proto_file}")
            subprocess.run([
                "python3", "-m", "grpc_tools.protoc",
                f"-I={downloaded_proto_dir}",
                f"--python_out={current_path}",
                f"--grpc_python_out={current_path}",
                proto_file
            ])
            logger.info(f"Generated gRPC code for {proto_file}")
        except Exception as e:
            logger.error(f"Failed to generate gRPC code for {proto_file}: {e}")

    def add_init_py_to_com_folders(self):
        """Add __init__.py files to all subdirectories in the generated com folder."""
        com_dir = os.path.join(current_path, "com")  # This is the root folder for gRPC-generated code
        
        if not os.path.exists(com_dir):
            print(f"'com' directory not found. gRPC generation might have failed.")
            return
    
        # Create __init__.py file folder
        self.create_init_file(com_dir)
        
        # Walk through com directory and ensure each folder has an __init__.py
        for root, dirs, _ in os.walk(com_dir):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                self.create_init_file(dir_path)
    
    def create_init_file(self, path):
        """Create an __init__.py file in the given path."""
        init_file_path = os.path.join(path, "__init__.py")
        if not os.path.exists(init_file_path):
            with open(init_file_path, 'w') as f:
                f.write('')
            print(f"Created: {init_file_path}")


PrepareGrpcPackages().run()

setup(
    name='multicorn_das',
    use_scm_version=True,  # Automatically use the Git version
    setup_requires=['setuptools_scm'],
    packages=[
        "com",
        "com.rawlabs",
        "com.rawlabs.protocol",
        "com.rawlabs.protocol.das",
        "com.rawlabs.protocol.das.v1",
        "com.rawlabs.protocol.das.v1.common",
        "com.rawlabs.protocol.das.v1.functions",
        "com.rawlabs.protocol.das.v1.tables",
        "com.rawlabs.protocol.das.v1.types",
        "com.rawlabs.protocol.das.v1.query",
        "com.rawlabs.protocol.das.v1.services",
        "multicorn_das"
    ],
    exclude_package_data={
        '': ['licenses/*', 'downloaded/*'],  # Exclude any files in these folders
    },
    install_requires=[
        'protobuf',
        'googleapis-common-protos',
        'grpcio'
    ],
    entry_points={
        'console_scripts': [
            # 'script_name=module_name:function_name',
        ],
    },
    author='Miguel Branco',
    author_email='miguel@raw-labs.com',
    description='DAS implementation using custom fork of Multicorn2',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/raw-labs/multicorn-das',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)
