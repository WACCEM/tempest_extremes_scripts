#!/usr/bin/env python3

import argparse

from utils.io_utilities import (
    ensure_dir,
    file_cleanup_recipe,
    load_config_and_generate_files,
    run_recipes,
    setup_env,
)

IO_CONFIG = "projects/prescCRE_extreme_precip/io_config_v3.LR.F2010.control_testing.yaml"


def main():
    parser = argparse.ArgumentParser(
        description="Run the TempestExtremes recipes declared in the io config."
    )
    parser.add_argument("--io-config", default=IO_CONFIG, help="Path to the io config YAML")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build the file lists and print the commands without running them")
    args = parser.parse_args()

    setup_env()

    config = load_config_and_generate_files(args.io_config)
    ensure_dir(config['output_dir'])

    resolved, artifacts = run_recipes(config, dry_run=args.dry_run)

    if not args.dry_run:
        file_cleanup_recipe(config, artifacts, drop_vars=[])

if __name__ == "__main__":
    main()
