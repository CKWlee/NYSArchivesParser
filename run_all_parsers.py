import subprocess
import sys

def main():
    # Use the current Python interpreter
    python_exec = sys.executable

    # 1) Raw formatting
    try:
        subprocess.check_call([python_exec, 'parse_raw_formatter.py'])
    except subprocess.CalledProcessError as e:
        print('Error in raw formatting:', e)
        sys.exit(1)

    # 2) Decode all files
    try:
        subprocess.check_call([python_exec, 'parse_decoded.py'])
    except subprocess.CalledProcessError as e:
        print('Error in decoding step:', e)
        sys.exit(1)

    # 3) Build the general Histpun aggregate
    try:
        subprocess.check_call([python_exec, 'build_histpun.py'])
    except subprocess.CalledProcessError as e:
        print('Error in building histpun output:', e)
        sys.exit(1)

    # 4) Build Institution × Court breakdown
    try:
        subprocess.check_call([python_exec, 'build_inst_court.py'])
    except subprocess.CalledProcessError as e:
        print('Error in building institution × court output:', e)
        sys.exit(1)

    # 5) Build Institution × County breakdown
    try:
        subprocess.check_call([python_exec, 'build_inst_county.py'])
    except subprocess.CalledProcessError as e:
        print('Error in building institution × county output:', e)
        sys.exit(1)

    print('All parsing and aggregation steps completed successfully.')

if __name__ == '__main__':
    main()
