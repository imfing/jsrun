"""Command-line interface for jsrun."""

import argparse
import sys
from pathlib import Path

from jsrun import JavaScriptError, Runtime, undefined


def main() -> None:
    """Main entry point for the jsrun CLI."""
    parser = argparse.ArgumentParser(
        description="Execute JavaScript code using jsrun",
        prog="python -m jsrun",
    )
    parser.add_argument(
        "-c",
        "--command",
        dest="command",
        help="Execute JavaScript code from command line",
    )
    parser.add_argument(
        "file",
        nargs="?",
        help="JavaScript file to execute",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.command and args.file:
        parser.error("Cannot specify both -c and a file argument")
    if not args.command and not args.file:
        parser.error("Must specify either -c or a file argument")

    try:
        with Runtime() as rt:
            if args.command:
                # Execute code from command line
                result = rt.eval(args.command)
                if result is not undefined:
                    print(result)
            else:
                # Execute file
                file_path = Path(args.file)
                if not file_path.exists():
                    print(f"Error: File '{file_path}' not found", file=sys.stderr)
                    sys.exit(1)

                code = file_path.read_text(encoding="utf-8")
                result = rt.eval(code)
                if result is not undefined:
                    print(result)
    except JavaScriptError as e:
        print(f"JavaScript Error: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
