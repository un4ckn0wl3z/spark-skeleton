import sys
import os
import libs.argparse
from core.logger import log
from core.configuration import Configuration
from core.spark import SparkManager


def init_dependencies():
    log.info("=== Initial libs and modules ===")

    if os.path.exists('config.zip'):
        log.info("=== Found config.zip ===")
        sys.path.insert(0, 'config.zip')
        log.info("=== Added config.zip to path ===")
    else:
        sys.path.insert(0, './config')
        log.info("=== Added ./config to path ===")

    if os.path.exists('core.zip'):
        log.info("=== Found core.zip ===")
        sys.path.insert(0, 'core.zip')
        log.info("=== Added core.zip to path ===")
    else:
        sys.path.insert(0, './core')
        log.info("=== Added ./core to path ===")

    if os.path.exists('jars.zip'):
        log.info("=== Found jars.zip ===")
        sys.path.insert(0, 'jars.zip')
        log.info("=== Added jars.zip to path ===")
    else:
        sys.path.insert(0, './jars')
        log.info("=== Added ./jars to path ===")

    if os.path.exists('jobs.zip'):
        log.info("=== Found jobs.zip ===")
        sys.path.insert(0, 'jobs.zip')
        log.info("=== Added jobs.zip to path ===")
    else:
        sys.path.insert(0, './jobs')
        log.info("=== Added ./jobs to path ===")

    if os.path.exists('libs.zip'):
        log.info("=== Found libs.zip ===")
        sys.path.insert(0, 'libs.zip')
        log.info("=== Added libs.zip to path ===")
    else:
        sys.path.insert(0, './libs')
        log.info("=== Added ./libs to path ===")

    if os.path.exists('log.zip'):
        log.info("=== Found log.zip ===")
        sys.path.insert(0, 'log.zip')
        log.info("=== Added log.zip to path ===")
    else:
        sys.path.insert(0, './log')
        log.info("=== Added ./log to path ===")


def init_args_parser():
    parser = libs.argparse.ArgumentParser()
    parser.add_argument("--env", help="set environment for app")
    args = parser.parse_args()

    if args.env is None:
        log.error("Error: Please enter your ENV")
        sys.exit(1)
    return args


def run():
    init_dependencies()
    log.info("=== Singularity ===")
    args = init_args_parser()
    log.info(f'Current environment: {args.env}')
    config = Configuration(args.env)
    log.info("=== Spark project is running... ===")
    log.info("== Configurations ==")

    log.info(f'app_name: {config.app_name}')
    log.info(f'input_mongodb_uri: {config.input_mongodb_uri}')
    log.info(f'output_mongodb_uri: {config.output_mongodb_uri}')
    log.info(f'jars_dir: {config.jars_dir}')
    log.info(f'master: {config.master}')

    sp = SparkManager(config)
    sp.run()


if __name__ == '__main__':
    run()
