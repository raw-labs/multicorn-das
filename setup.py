import os
import subprocess
from setuptools import setup, find_packages
from setuptools.command.build_py import build_py as _build_py
from urllib.parse import urlparse

import requests

RAW_VERSION = "0.38.0"

DAS_VERSION = "main"

PROTO_FILES = [
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/protocol-raw/src/main/protobuf/com/rawlabs/protocol/raw/types.proto",
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/protocol-raw/src/main/protobuf/com/rawlabs/protocol/raw/values.proto",
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/src/main/protobuf/com/rawlabs/protocol/das/das.proto",
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/src/main/protobuf/com/rawlabs/protocol/das/tables.proto",
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/src/main/protobuf/com/rawlabs/protocol/das/functions.proto",
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/src/main/protobuf/com/rawlabs/protocol/das/services/registration_service.proto",
    f"https://raw.githubusercontent.com/raw-labs/snapi/{RAW_VERSION}/src/main/protobuf/com/rawlabs/protocol/das/services/tables_service.proto"
]

LOCAL_PROTO_DIR = "downloaded"

# Custom command class to download .proto files and generate gRPC Python code
class CustomBuild(_build_py):
    
    def run(self):
        self.create_local_proto_dir()
        self.download_proto_files()
        self.generate_grpc_code()
        super().run()

    def create_local_proto_dir(self):
        """Ensure the local proto directory exists."""
        os.makedirs(LOCAL_PROTO_DIR, exist_ok=True)

    def download_proto_files(self):
        """Download proto files and recreate the directory structure."""
        for proto_file_url in PROTO_FILES:
            local_file_path = self.get_local_file_path(proto_file_url)
            if not local_file_path:
                continue

            # Create directories and __init__.py in each one
            self.create_dirs_with_init(os.path.dirname(local_file_path))
            self.download_file(proto_file_url, local_file_path)

    def get_local_file_path(self, proto_file_url):
        """Parse the URL to get the path and filename, and recreate local structure."""
        parsed_url = urlparse(proto_file_url)
        proto_path_parts = parsed_url.path.split('/')

        try:
            com_index = proto_path_parts.index('com')
        except ValueError:
            return None

        return os.path.join(LOCAL_PROTO_DIR, *proto_path_parts[com_index:])

    def download_file(self, url, local_path):
        """Download the file from the given URL and save it locally."""
        response = requests.get(url)
        if response.status_code == 200:
            with open(local_path, 'wb') as f:
                f.write(response.content)
            return True
        else:
            return False

    def generate_grpc_code(self):
        """Generate gRPC Python code from all proto files."""
        for proto_file in self.find_proto_files():
            self.run_protoc(proto_file)

    def find_proto_files(self):
        """Find all .proto files in the local proto directory."""
        for root, _, files in os.walk(LOCAL_PROTO_DIR):
            for file in files:
                if file.endswith('.proto'):
                    yield os.path.join(root, file)

    def run_protoc(self, proto_file):
        """Run the protoc command to generate Python gRPC code for a given proto file."""
        subprocess.check_call([
            "python3", "-m", "grpc_tools.protoc",
            f"-I={LOCAL_PROTO_DIR}",
            f"--python_out=.",
            f"--grpc_python_out=.",
            proto_file
        ])

    def create_dirs_with_init(self, dir_path):
        """Create directories and ensure each has an __init__.py file."""
        os.makedirs(dir_path, exist_ok=True)
        current_dir = dir_path

        # Walk up the directory tree from the leaf to the root, adding __init__.py
        while current_dir != LOCAL_PROTO_DIR:
            init_file_path = os.path.join(current_dir, "__init__.py")
            if not os.path.exists(init_file_path):
                with open(init_file_path, 'w') as f:
                    f.write('')  # Create an empty __init__.py
            current_dir = os.path.dirname(current_dir)  # Move up one level


setup(
    name='multicorn_das',
    use_scm_version=True,  # Automatically use the Git version
    setup_requires=['setuptools_scm'],
    packages=[
        'multicorn_das',
        'com',
        'com.rawlabs',
        'com.rawlabs.protocol',
        'com.rawlabs.protocol.das',
        'com.rawlabs.protocol.das.services',
        'com.rawlabs.protocol.raw',
    ],
    install_requires=[
        'protobuf',
        'googleapis-common-protos',
        'grpcio'
    ],
    cmdclass={
        'build_py': CustomBuild,  # Use the custom build command
    },    
    entry_points={
        'console_scripts': [
            # 'script_name=module_name:function_name',
        ],
    },
    author='Miguel Branco',
    author_email='miguel@raw-labs.com',
    description='DAS implementation using Multicorn2',
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
