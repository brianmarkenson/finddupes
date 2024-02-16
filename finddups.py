import os
import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
import pickle
from multiprocessing import Manager

manager = Manager()
hash_dict = manager.dict()
file_dict = manager.dict()
file_dict_lock = threading.Lock()

log_lock = threading.Lock() # Used for logfile.txt
duplicate_lock = threading.Lock() # Used for the duplicate_files.txt file
hardlinked_inodes_lock = threading.Lock() # Used for hardlinks.txt
nfs_mount_sizes_lock = threading.Lock() # Used for nfs_mount_sizes.txt

# Create a dictionary to store thread-specific states
thread_states = {}

def save_state(filename, state):
    with open(filename, 'wb') as f:
        pickle.dump(state, f)

def load_state(filename):
    try:
        with open(filename, 'rb') as f:
            return pickle.load(f)
    except FileNotFoundError:
        return None

# Load the previously saved states, if available
loaded_state = load_state('saved_state.pkl')
if loaded_state:
    hash_dict, file_dict, hardlinked_inodes, nfs_mount_sizes = loaded_state

def print_thread_id(action):
    thread_id = threading.get_ident()
    print(f"Thread ID {thread_id} {action}: ")

# Overwrite files rather than appending if required
def prompt_overwrite(filename):
    if os.path.exists(filename):
        choice = input(f"{filename} already exists. Overwrite? (y/n): ")
        if choice.lower() == 'y':
            open(filename, 'w').close()

prompt_overwrite('logfile.txt')
prompt_overwrite('duplicate_files.txt')
prompt_overwrite('nfs_mount_sizes.txt')
prompt_overwrite('hardlinks.txt')


def log(msg, newline=True):
    thread_id = threading.get_ident()
    with log_lock:
        with open('logfile.txt', 'a') as f:
            if newline:
                f.write(f"{thread_id}: {msg}\n")
            else:
                f.write(f"{thread_id}: {msg} ")

def calculate_hash(file_path, chunk_size=1024, max_bytes=None):
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        bytes_read = 0
        while chunk := f.read(chunk_size):
            hasher.update(chunk)
            bytes_read += len(chunk)
            if max_bytes is not None and bytes_read >= max_bytes:
                break
    return hasher.hexdigest()

hardlinked_inodes = {}

def find_duplicates(base_path):
    global file_dict
    global hash_dict
    thread_id = threading.get_ident()
    thread_local = threading.local()
    thread_states[thread_id] = {
        'hardlinked_inodes': {},
        'nfs_mount_sizes': {}
    }
    print_thread_id(f"Starting {base_path}")
    hardlinked_inodes = getattr(thread_local, 'hardlinked_inodes', {})
    nfs_mount_sizes = getattr(thread_local, 'nfs_mount_sizes', {})

    def save_thread_state():
        thread_states[thread_id]['hardlinked_inodes'] = hardlinked_inodes.copy()
        thread_states[thread_id]['nfs_mount_sizes'] = nfs_mount_sizes.copy()

    def restore_thread_state():
        hardlinked_inodes = thread_states[thread_id]['hardlinked_inodes']
        nfs_mount_sizes = thread_states[thread_id]['nfs_mount_sizes']

    try:
        # Attempt to restore thread-specific state from loaded_state
        if loaded_state and thread_id in loaded_state:
            thread_state = loaded_state[thread_id]
            thread_states[thread_id]['hardlinked_inodes'] = thread_state.get('hardlinked_inodes', {})
            thread_states[thread_id]['nfs_mount_sizes'] = thread_state.get('nfs_mount_sizes', {})

            hardlinked_inodes, nfs_mount_sizes = (
                thread_state['hardlinked_inodes'], thread_state['nfs_mount_sizes']
            )

    # Determine the filesystem based on the base_path
        nfs_mount = os.path.realpath(base_path)

        # Initialize the dictionaries for this filesystem if not already done
        if nfs_mount not in hardlinked_inodes:
            hardlinked_inodes[nfs_mount] = set()
        if nfs_mount not in nfs_mount_sizes:
            nfs_mount_sizes[nfs_mount] = 0

        for foldername, _, filenames in os.walk(base_path):
            for filename in filenames:
                filepath = os.path.join(foldername, filename)
                try:
                    if os.path.isfile(filepath):  # Check if it's a regular file
                        file_size = os.path.getsize(filepath)

                        if file_size not in file_dict:
                            with file_dict_lock:
                                file_dict[file_size] = []
                        with file_dict_lock:
                            file_dict[file_size].append(filepath)

                        # Check for hardlinks using inode numbers
                        file_inode = os.stat(filepath).st_ino

                        # Check for hardlinks within this filesystem
                        if file_inode not in hardlinked_inodes[nfs_mount]:
                            # Log the "Checking filename" and the note on the same line
                            log(f"Checking {filepath}:", newline=False)
                            if len(file_dict[file_size]) > 1:
                                log(f"Matching size {file_size}")
                            else:
                                log("")  # Add a newline when file size doesn't match
                        else:
                            # Log as hardlinked but not as a duplicate
                            log(f"Hardlinked file found: {filepath}", newline=True)

                        # Add the inode to the set of hardlinked inodes for this filesystem
                        hardlinked_inodes[nfs_mount].add(file_inode)

                        # Track total sizes for this NFS mount
                        nfs_mount_sizes[nfs_mount] += file_size

                    else:
                        log(f"Skipping {filepath} - not a regular file")
                except Exception as e:
                    log(f"Error reading {filepath}: {e}")

        total_possibilities = sum(len(file_paths) for file_paths in file_dict.values())
        current_position = 0
        for file_size, file_paths in file_dict.items():
            if len(file_paths) > 1:
                local_hash_dict = {}
                for file_path in file_paths:
                    file_hash = calculate_hash(file_path, chunk_size=4096, max_bytes=16 * 1024)
                    if file_hash not in local_hash_dict:
                        local_hash_dict[file_hash] = []
                    local_hash_dict[file_hash].append(file_path)

                for file_hash, duplicate_paths in local_hash_dict.items():
                    if len(duplicate_paths) > 1:
                        current_position += 1
                        log(f"{current_position}/{total_possibilities}: ", newline=False)
                        # Log a single line indicating a duplicate was found with duplicate file paths
                        duplicate_info = f"Hash {file_hash}: {', '.join(duplicate_paths)}"
                        log(duplicate_info)
                        print(duplicate_info)

                        with duplicate_lock:
                            if file_hash not in hash_dict:
                                hash_dict[file_hash] = []
                            hash_dict[file_hash].extend(duplicate_paths)

                            # Write duplicate_info to "duplicate_files.txt"
                            with open('duplicate_files.txt', 'a') as f:
                                f.write(f"Possible duplicate files for hash {file_hash}:\n")
                                for duplicate_path in duplicate_paths:
                                    f.write(f"  {duplicate_path}\n")
        print_thread_id(f"Finished {base_path}")

    except Exception as e:
        log(f"Error in thread {thread_id}: {e}")
    finally:
        # Save the thread-specific state before exiting
        save_thread_state()
        log(f"Thread {thread_id} finished")

    # Save the global state after all threads finish
    if thread_id == main_thread_id:
        save_state('saved_state.pkl', {
            'hash_dict': hash_dict,
            'file_dict': file_dict,
            'hardlinked_inodes': hardlinked_inodes,
            'nfs_mount_sizes': nfs_mount_sizes
        })


    # Print NFS mount size for this thread
    nfs_mount = os.path.realpath(base_path)
    with nfs_mount_sizes_lock:  # You should define nfs_size_lock as a threading.Lock
        print(f"Thread {thread_id} NFS mount size for {nfs_mount}: {nfs_mount_sizes[nfs_mount]} bytes")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=3) as executor:
        base_paths = ['/n/central', '/n/bgmqnap02', '/n/wd01']
        executor.map(find_duplicates, base_paths)

    # Log hardlinked files to "hardlinks.txt"

    # Log hardlinked files to "hardlinks.txt"
    with open('hardlinks.txt', 'w') as f:
        f.write("Hardlinked files:\n")
        for inode in hardlinked_inodes:
            linked_files = [file_path for file_path in file_dict[inode]]
            f.write("\n".join(linked_files))
            f.write("\n\n")

    # Log NFS mount sizes to "nfs_mount_sizes.txt"
    with open('nfs_mount_sizes.txt', 'w') as f:
        f.write("Total sizes for each NFS mount:\n")
        for nfs_mount, total_size in nfs_mount_sizes.items():
            f.write(f"{nfs_mount}: {total_size} bytes\n")
