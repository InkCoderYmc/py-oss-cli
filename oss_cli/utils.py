import os

def check_file_exists(file_path):
    if os.path.exists(file_path):
        # print(f"The file '{file_path}' exists.")
        return True
    else:
        # print(f"The file '{file_path}' does not exist.")
        return False