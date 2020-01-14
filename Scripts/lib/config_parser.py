"""
This module is used to allow a uniform command line interface for all scripts in ThePit

To override something in the yaml file just add another argument to the cli and not the script itself, these must have
an equals and are denoted by -D in front of the name ex. -Da.b.c=1 which will set a.b.c=1 in the config, all other
values will be ignored
"""

import yaml
import argparse
import sys
import logging
import pathlib


# TODO is this necessary ?
class ConfigNode(dict):

    def get_config(self):
        return self

    def __getattr__(self, item: str):
        if '.' in item:
            vals = item.split('.')
            temp = self
            for v in vals:
                temp = temp[v]
            return temp
        else:
            return self[item]


def config_argparse(parser: argparse.ArgumentParser) -> ConfigNode:
    """general wrapper for the parser that will return a ConfigNode which is basically just a dict"""
    add_required_params(parser)
    args, unkown_args = parser.parse_known_args()

    config_path = pathlib.Path(args.config)

    if not config_path.exists():
        logging.error(f"Config path {args.config} does not exist")
        sys.exit(1)

    with open(args.config) as f:
        config = yaml.full_load(f)

    config['args'] = vars(args)

    if 'importing' in config:
        for k, v in config['importing'].items():
            logging.info(f"Adding files for {k} into config_argparse")
            with open(config_path.parent / v) as f:
                config[k] = yaml.full_load(f)

    config_node = ConfigNode(config)
    map_config_overrides(unkown_args, config_node)

    return ConfigNode(config)


def map_config_overrides(unkown_args: list, config_node: ConfigNode):
    """This function will take the args that it did not know about and try to map them to a value in the dict, it will
    only warn if it gets something that it doesnt recognize, uses the dot notation for ConfigNode only"""
    for arg in unkown_args:
        if '-D' not in arg:
            logging.warning(f"{arg} does not contain '-D' please append with letter to overwrite args")
            continue
        checker = arg.replace('-D', '')
        name, value = checker.split('=')
        if name in config_node:
            logging.info(f"Updating config for {name} to be {value}")
            config_node[name] = value
        else:
            logging.warning(f"{name} is not in the config file ignoring")


def add_required_params(parser: argparse.ArgumentParser):
    parser.add_argument('-c', '--config', help='Path to the config file', required=True)
    parser.add_argument('-s', '--site', help='Environment and configs to use', choices=['PROD', 'QA', 'DEV'])
