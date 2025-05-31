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

    # 2) Single pass decoding (decode all files at once)
    try:
        subprocess.check_call([python_exec, 'parse_decoded.py'])
    except subprocess.CalledProcessError as e:
        print('Error in decoding step:', e)
        sys.exit(1)

    print('All parsing steps completed successfully.')

if __name__ == '__main__':
    main()