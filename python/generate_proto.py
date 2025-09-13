#!/usr/bin/env python3
"""
Script to generate protobuf Python files from .proto definitions
"""

import os
import subprocess
import sys
from pathlib import Path

def generate_protobuf_files():
    """Generate protobuf files from .proto definitions"""
    
    # Get project root directory
    project_root = Path(__file__).parent.parent
    proto_dir = project_root / "proto"
    output_dir = project_root / "python" / "src" / "generated"
    
    print(f"Project root: {project_root}")
    print(f"Proto directory: {proto_dir}")
    print(f"Output directory: {output_dir}")
    
    # Check if proto file exists
    proto_file = proto_dir / "market_surveillance.proto"
    if not proto_file.exists():
        print(f"Error: Proto file not found at {proto_file}")
        return False
    
    # Create output directory if it doesn't exist
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create __init__.py file in generated directory
    init_file = output_dir / "__init__.py"
    if not init_file.exists():
        init_file.touch()
    
    try:
        # Generate Python protobuf files
        cmd = [
            sys.executable, "-m", "grpc_tools.protoc",
            f"--proto_path={proto_dir}",
            f"--python_out={output_dir}",
            f"--grpc_python_out={output_dir}",
            str(proto_file)
        ]
        
        print(f"Running command: {' '.join(cmd)}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        
        print("Protobuf files generated successfully!")
        print(f"Generated files in: {output_dir}")
        
        # List generated files
        for file in output_dir.glob("*.py"):
            print(f"  - {file.name}")
            
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"Error generating protobuf files: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def main():
    """Main function"""
    print("Generating protobuf files for Market Surveillance Service...")
    
    if generate_protobuf_files():
        print("rotobuf generation completed successfully!")
        return 0
    else:
        print("Protobuf generation failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())