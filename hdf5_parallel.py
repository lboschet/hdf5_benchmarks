import os
import socket
import numpy as np
import time
import argparse
import h5py
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

###
### HDF5 testsings
###
# Example (generic) : 
#    python3 hdf5-parallel.py --file_size 2.0 --bandwidth 1.0 --num_files 5 --output_directory /path/to/output
# Example (pure) : 
#    python3 ./hdf5_parallel.py --file_size 10.0 --bandwidth 8.0 --num_files 1 --output_directory /hdf5


def generate_large_hdf5_file(file_path, data_shape):
    # Generate random dat with numpy
    data = np.random.random(size=data_shape)
    print("Numpy shape:", data_shape) 

    with h5py.File(file_path, 'w') as file:
        # Create a random dataset in the HDF5 file
        file.create_dataset('random_data', data=data)

# Function to generate a single HDF5 file
def generate_and_write_file(file_path, data_shape):
    
    start_time = time.time()
    print("\tStart Time:", datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H:%M:%S"))

    # Generate the HDF5 data here
    data = np.random.random(size=data_shape)
    #print("data :", data[:100]) 
    print("\tNumpy shape:", data_shape) 

    with h5py.File(file_path, 'w') as file:
        # Create a random dataset in the HDF5 file
        file.create_dataset('random_data', data=data)

    end_time = time.time()
    print("\tEnd Time:", datetime.utcfromtimestamp(end_time).strftime("%Y-%m-%d %H:%M:%S"))

    elapsed_time = end_time - start_time
    print("\tElapsed Time (s): {:.2f}".format(elapsed_time))

    return elapsed_time


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Generate large HDF5 files with parallel processing.')
    parser.add_argument('--file_size', type=float, default=1.0, help='Size of each file in gigabytes.')
    parser.add_argument('--bandwidth', type=float, default=0.5, help='Bandwidth in gigabytes per second.')
    parser.add_argument('--num_files', type=int, default=3, help='Number of files to generate.')
    parser.add_argument('--output_directory', type=str, default='/path/to/your/directory', help='Output directory for generated files.')

    args = parser.parse_args()

    # Get the hostname
    host_name = socket.gethostname()

    # App parameters
    file_size_GB = args.file_size
    bandwidth_GB_per_sec = args.bandwidth
    num_files = args.num_files
    output_directory = args.output_directory

    #data_shape = (int(file_size_GB * 1e9 / 8), int(file_size_GB * 1e9 / 8000000))
    data_shape = (int(file_size_GB * 1e9 / 8), )

    # Calculate time required per file
    time_per_file_sec = file_size_GB / bandwidth_GB_per_sec

    # Use ThreadPoolExecutor to parallelize file processing
    with ThreadPoolExecutor() as executor:
        futures = []

        for i in range(1, num_files + 1):
            # Include the hostname in the file name
            file_name = f"large_hdf5_file_{host_name}_{i}.h5"
            file_path = os.path.join(output_directory, file_name)

            # Submit the job to the executor pool
            print(f'Submit job number {i} for file: {file_path}')
            future = executor.submit(generate_and_write_file, file_path, data_shape)
            futures.append((file_name, future))
            

        # Wait for all tasks to complete
        for file_name, future in futures:
            elapsed_time = future.result()

            # Wait for the time required to achieve the desired bandwidth
            if elapsed_time < time_per_file_sec:
                time.sleep(time_per_file_sec - elapsed_time)

            print(f"File {file_name} generated successfully.")

if __name__ == "__main__":
    main()
