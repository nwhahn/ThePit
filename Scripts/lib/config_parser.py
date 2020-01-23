"""
This module is used to allow a uniform command line interface for all scripts in ThePit

To override something in the yaml file just add another argument to the cli and not the script itself, these must have
an equals and are denoted by -D in front of the name ex. -Da.b.c=1 which will set a.b.c=1 in the config, all other
values will be ignored

This does not support floats from the command line
"""

import yaml
import argparse
import sys
import logging
import pathlib


# TODO make each dict in here a ConfigNode itself
class ConfigNode(dict):

    def get_config(self):
        return self

    def __getitem__(self, item: str):
        if '.' in item:
            vals = item.split('.')
            temp = self
            for v in vals:
                temp = temp[v]
            return temp
        else:
            return dict.__getitem__(self, item)

    def __setitem__(self, item: str, value):
        if '.' in item:
            vals = item.split('.')
            setter = vals[-1]
            temp = self
            for v in vals:
                temp = temp[v]
                if setter in temp:
                    if value.isnumeric():
                        temp[setter] = int(value)
                    else:
                        temp[setter] = value
                    break

    def __contains__(self, item: str):
        try:
            self[item]
            rv = True
        except KeyError:
            rv = False
        return rv


def config_argparse(parser: argparse.ArgumentParser) -> ConfigNode:
    """general wrapper for the parser that will return a ConfigNode which is basically just a dict"""
    add_required_params(parser)
    args, unkown_args = parser.parse_known_args()

    config_path = pathlib.Path(args.config)

    if not config_path.exists():
        logging.error(f"Config path {args.config} does not exist")
        sys.exit(1)

    with open(args.config) as f:
        config = yaml.load(f)

    config['args'] = vars(args)

    if 'importing' in config:
        for k, v in config['importing'].items():
            logging.info(f"Adding files for {k} into config_argparse")
            with open(config_path.parent / v) as f:
                config[k] = yaml.load(f)

    config_node = ConfigNode(config)
    map_config_overrides(unkown_args, config_node)

    log_config(config_node)
    return config_node


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


def log_config(config_node: ConfigNode) -> None:
    def log_dict(config_dict: dict, recurse_num):
        for key, value in config_dict.items():
            if isinstance(value, dict):
                logging.info(f"{' ' * recurse_num * 2}[{key}]")
                log_dict(value, recurse_num + 1)
            else:
                logging.info(f"{' ' * (recurse_num * 3)}{key} = {value}")
    for k, v in config_node.items():
        if isinstance(v, dict):
            logging.info(f"[{k}]")
            log_dict(v, 1)
        else:
            logging.info(f"{' ' * 2}{k} = {v}")
